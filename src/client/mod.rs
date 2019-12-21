mod value_types;
pub use value_types::{
    KeepAlive,
    Publish,
    ReadResult,
    Subscribe,
    SubscribeResult,
};

mod builder;
pub use builder::ClientBuilder;

pub use mqttrs::{
    QoS,
    SubscribeReturnCodes,
    SubscribeTopic,
};

use bytes::BytesMut;
use crate::{
    Error,
    Result,
    util::{
        FreePidList,
        TokioRuntime,
    }
};
use futures_util::{
    future::{
        FutureExt,
        pending,
    },
    select,
};
use log::{debug, error, trace};
use mqttrs::{
    ConnectReturnCode,
    Packet,
    Pid,
    QosPid,
    self
};
use std::{
    collections::BTreeMap,
    net::Shutdown,
};
use tokio::{
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
    net::TcpStream,
    sync::{
        mpsc,
        oneshot,
    },
    time::{
        delay_until,
        Duration,
        Instant,
    },
};

/// An arbitrary value that should perhaps be configurable.
const MAX_PACKET_SIZE: usize = 65536;

pub struct Client {
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<Vec<u8>>,
    keep_alive: KeepAlive,
    runtime: TokioRuntime,
    client_id: Option<String>,

    state: ConnectState,
    free_write_pids: FreePidList,
}

enum ConnectState {
    Disconnected,
    Connected(ClientConnection)
}

struct ClientConnection {
    tx_write_requests: mpsc::Sender<IoRequest>,
    rx_recv_published: mpsc::Receiver<Packet>,
}

struct IoTask {
    keep_alive: KeepAlive,

    stream: TcpStream,
    rx_write_requests: mpsc::Receiver<IoRequest>,
    tx_recv_published: mpsc::Sender<Packet>,

    /// The time the last packet was written to `stream`.
    /// Used to calculate when to send a Pingreq
    last_write_time: Instant,

    pid_response_map: BTreeMap<Pid, IoRequest>,
    connack_response: Option<IoRequest>,
}

#[derive(Debug)]
struct IoRequest {
    packet: Packet,
    tx_result: oneshot::Sender<IoResult>,
    io_type: IoType,
}

#[derive(Debug)]
enum IoType {
    WriteOnly,
    WriteAndResponse { response_pid: Pid },
    WriteConnect,
}

#[derive(Debug)]
struct IoResult {
    result: Result<Option<Packet>>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub async fn connect(&mut self) -> Result<()> {
        self.check_disconnected()?;
        let stream = TcpStream::connect((&*self.host, self.port))
            .await?;
        let (tx_write_requests, rx_write_requests) = mpsc::channel::<IoRequest>(100);
        // TODO: Change this to allow control messages, e.g. disconnected?
        let (tx_recv_published, rx_recv_published) = mpsc::channel::<Packet>(100);
        self.state = ConnectState::Connected(ClientConnection {
            tx_write_requests,
            rx_recv_published,
        });
        let io = IoTask {
            keep_alive: self.keep_alive,
            stream,
            rx_write_requests,
            tx_recv_published,
            last_write_time: Instant::now(),
            pid_response_map: BTreeMap::new(),
            connack_response: None,
        };
        self.runtime.spawn(io.run());

        let conn = Packet::Connect(mqttrs::Connect {
            protocol: mqttrs::Protocol::MQTT311,
            keep_alive: match self.keep_alive {
                KeepAlive::Disabled => 0,
                KeepAlive::Enabled { secs } => secs,
            },
            client_id: match &self.client_id {
                None => "".to_owned(),
                Some(cid) => cid.to_owned(),
            },
            clean_session: true, // TODO
            last_will: None, // TODO
            username: self.username.clone(),
            password: self.password.clone(),
        });
        let connack = self.write_connect(&conn).await?;
        // TODO: timeout on CONNACK
        match connack {
            Packet::Connack(ca) => {
                if ca.code != ConnectReturnCode::Accepted {
                    let _ = self.shutdown().await;
                    return Err(format!("Bad connect return code: {:?}", ca.code).into());
                }
            },
            _ => {
                self.shutdown().await?;
                return Err("Received packet not CONNACK after connect".into());
            }
        }
        Ok(())
    }

    pub async fn publish(&mut self, p: Publish) -> Result<()> {
        let qos = p.qos();
        if qos == QoS::ExactlyOnce {
            return Err("QoS::ExactlyOnce is not supported".into());
        }
        let p2 = Packet::Publish(mqttrs::Publish {
            dup: false, // TODO.
            qospid: match qos {
                QoS::AtMostOnce => QosPid::AtMostOnce,
                QoS::AtLeastOnce => QosPid::AtLeastOnce(self.alloc_write_pid()?),
                QoS::ExactlyOnce => panic!("Not reached"),
            },
            retain: false, // TODO
            topic_name: p.topic().to_owned(),
            payload: p.payload().to_owned(),
        });
        match qos {
            QoS::AtMostOnce => self.write_only_packet(&p2).await?,
            QoS::AtLeastOnce => {
                let resp = self.write_response_packet(&p2).await?;
                match resp {
                    Packet::Puback(pid) => self.free_write_pid(pid)?,
                    _ => error!("Bad packet response for publish: {:#?}", resp),
                }
            },
            QoS::ExactlyOnce => panic!("Not reached"),
        };
        Ok(())
    }

    pub async fn subscribe(&mut self, s: Subscribe) -> Result<SubscribeResult> {
        let pid = self.alloc_write_pid()?;
        // TODO: Support subscribe to qos != AtMostOnce.
        if s.topics().iter().any(|t| t.qos != QoS::AtMostOnce) {
            return Err("Only Qos::AtMostOnce supported right now".into())
        }
        let p = Packet::Subscribe(mqttrs::Subscribe {
            pid: pid,
            topics: s.topics().to_owned(),
        });
        let r = self.write_response_packet(&p).await?;
        // TODO: Implement timeout.
        // TODO: BUG: Handle other packets.
        match r {
            Packet::Suback(mqttrs::Suback {
                pid: suback_pid,
                return_codes: rcs,
            }) if suback_pid == pid => {
                self.free_write_pid(pid)?;
                Ok(SubscribeResult {
                    return_codes: rcs
                })
            },
            _ => {
                return Err(format!("Unexpected packet waiting for Suback(Pid={:?}): {:#?}",
                                   pid, r)
                           .into());
            }
        }
    }

    pub async fn read_published(&mut self) -> Result<ReadResult> {
        let c = self.check_connected()?;
        let r = match c.rx_recv_published.recv().await {
            Some(r) => r,
            None => {
                // Sender closed.
                self.state = ConnectState::Disconnected;
                return Err(Error::Disconnected);
            }
        };
        match r {
            Packet::Publish(p) => {
                if p.qospid != QosPid::AtMostOnce {
                    panic!("Unimplemented QoS: {:?}", p.qospid.qos());
                }
                let rr = ReadResult {
                    topic: p.topic_name,
                    payload: p.payload,
                };
                Ok(rr)
            },
            _ => {
                return Err(format!("Unexpected packet waiting for read: {:#?}", r).into());
            }
        }
    }

    /// Gracefully close the connection to the server.
    pub async fn disconnect(&mut self) -> Result<()> {
        self.check_connected()?;
        let p = Packet::Disconnect;
        self.write_only_packet(&p).await?;
        self.shutdown().await?;
        Ok(())
    }

    fn alloc_write_pid(&mut self) -> Result<Pid> {
        match self.free_write_pids.alloc() {
            Some(pid) => Ok(Pid::try_from(pid).expect("Non-zero Pid")),
            None => Err(Error::from("No free Pids")),
        }
    }

    fn free_write_pid(&mut self, p: Pid) -> Result<()> {
        match self.free_write_pids.free(p.get()) {
            true => Err(Error::from("Pid was already free")),
            false => Ok(())
        }
    }

    async fn shutdown(&mut self) -> Result <()> {
        let _c = self.check_connected()?;
        // Setting the state drops the write end of the channel to the IoTask, which
        // will then shut down the stream.
        self.state = ConnectState::Disconnected;
        Ok(())
    }

    async fn write_only_packet(&mut self, p: &Packet) -> Result<()> {
        self.write_request(p, IoType::WriteOnly)
            .await.map(|_v| ())
    }

    async fn write_response_packet(&mut self, p: &Packet) -> Result<Packet> {
        let io_type = IoType::WriteAndResponse {
            response_pid: packet_pid(p).expect("packet_pid")
        };
        self.write_request(p, io_type)
            .await.map(|v| v.expect("return packet"))
    }

    async fn write_connect(&mut self, p: &Packet) -> Result<Packet> {
        self.write_request(p, IoType::WriteConnect)
            .await.map(|v| v.expect("return packet"))
    }

    async fn write_request(&mut self, p: &Packet, io_type: IoType) -> Result<Option<Packet>> {
        let c = self.check_connected()?;
        let (tx, rx) = oneshot::channel::<IoResult>();
        let req = IoRequest {
            packet: p.clone(),
            tx_result: tx,
            io_type: io_type,
        };
        c.tx_write_requests.send(req).await
            .map_err(|e| Error::from_std_err(e))?;
        // TODO: Add a timeout?
        let res = rx.await
            .map_err(|e| Error::from_std_err(e))?;
        res.result
    }

    fn check_connected(&mut self) -> Result<&mut ClientConnection> {
        match self.state {
            ConnectState::Disconnected => Err(Error::Disconnected),
            ConnectState::Connected(ref mut c) => Ok(c),
        }
    }

    fn check_disconnected(&mut self) -> Result<()> {
        match self.state {
            ConnectState::Disconnected => Ok(()),
            ConnectState::Connected(_) => Err("Connected already".into()),
        }
    }
}

fn packet_pid(p: &Packet) -> Option<Pid> {
    match p {
        Packet::Connect(_) => None,
        Packet::Connack(_) => None,
        Packet::Publish(publish) => publish.qospid.pid(),
        Packet::Puback(pid) => Some(pid.to_owned()),
        Packet::Pubrec(pid) => Some(pid.to_owned()),
        Packet::Pubrel(pid) => Some(pid.to_owned()),
        Packet::Pubcomp(pid) => Some(pid.to_owned()),
        Packet::Subscribe(sub) => Some(sub.pid),
        Packet::Suback(suback) => Some(suback.pid),
        Packet::Unsubscribe(unsub) => Some(unsub.pid),
        Packet::Unsuback(pid) => Some(pid.to_owned()),
        Packet::Pingreq => None,
        Packet::Pingresp => None,
        Packet::Disconnect => None,
    }
}

enum SelectResult {
    Req(Option<IoRequest>),
    Read(Result<Packet>),
    Ping,
}

impl IoTask {
    async fn run(mut self) {
        let IoTask { mut rx_write_requests, mut stream, mut last_write_time, .. } = self;
        loop {
            let keepalive_next = match &self.keep_alive {
                KeepAlive::Disabled => None,
                KeepAlive::Enabled{ secs } => {
                    let dur = Duration::from_secs(*secs as u64);
                    Some(last_write_time.checked_add(dur)
                         .expect("time addition to succeed"))
                },
            };

            let sel_res = {
                let mut req_fut = Box::pin(rx_write_requests.recv().fuse());
                let mut read_fut = Box::pin(Self::read_packet(&mut stream).fuse());
                let mut ping_fut = match keepalive_next {
                    Some(t) => Box::pin(delay_until(t).boxed().fuse()),
                    None => Box::pin(pending().boxed().fuse()),
                };
                select! {
                    read = read_fut => SelectResult::Read(read),
                    req = req_fut => SelectResult::Req(req),
                    ping = ping_fut => SelectResult::Ping,
                }
            };
            match sel_res {
                SelectResult::Read(read) =>
                    match read {
                        Err(Error::Disconnected) => {
                            return;
                        }
                        Err(e) => {
                            error!("IoTask: Failed to read packet: {}", e);
                        },
                        Ok(p) => {
                            match p {
                                Packet::Pingresp => {
                                    debug!("IoTask: Ignoring Pingresp");
                                    continue
                                },
                                Packet::Publish(_) => {
                                    if let Err(e) = self.tx_recv_published.send(p).await {
                                        error!("IoTask: Failed to send Packet: {}", e);
                                    }
                                },
                                Packet::Connack(_) => {
                                    if let Some(ca_req) = self.connack_response {
                                        trace!("Sending connack response p={:#?}",
                                               p);
                                        let res = IoResult { result: Ok(Some(p)) };
                                        if let Err(e) = ca_req.tx_result.send(res) {
                                            error!("IoTask: Failed to send IoResult: {:?}", e);
                                        }
                                    }
                                    self.connack_response = None;
                                }
                                _ => {
                                    let pid = packet_pid(&p);
                                    if let Some(pid) = pid {
                                        let pid_response = self.pid_response_map.remove(&pid);
                                        match pid_response {
                                            None => error!("Unknown PID: {:?}", pid),
                                            Some(req) => {
                                                trace!("Sending response PID={:?} p={:#?}",
                                                       pid, p);
                                                let res = IoResult { result: Ok(Some(p)) };
                                                if let Err(e) = req.tx_result.send(res) {
                                                    error!("IoTask: Failed to send IoResult: {:?}",
                                                           e);
                                                }
                                            },
                                        }
                                    }
                                },
                            }
                        },
                    },
                SelectResult::Req(req) => match req {
                    None => {
                        // TODO: Test sender closed.
                        // Sender closed.
                        if let Err(e) = stream.shutdown(Shutdown::Both) {
                            error!("IoTask: Error shutting down TcpStream: {}", e);
                        }
                        return;
                    },
                    Some(req) => {
                        last_write_time = Instant::now();
                        let res = Self::write_packet(&req.packet, &mut stream).await;
                        if let Err(ref e) = res {
                            error!("IoTask: Error writing packet: {:?}", e);
                            let res = IoResult { result: res.map(|_| None) };
                            if let Err(e) = req.tx_result.send(res) {
                                error!("IoTask: Failed to send IoResult: {:?}", e);
                            }
                            continue;
                        } else {
                            match req.io_type {
                                IoType::WriteOnly => {
                                    let res = IoResult { result: res.map(|_| None) };
                                    if let Err(e) = req.tx_result.send(res) {
                                        error!("IoTask: Failed to send IoResult: {:?}", e);
                                    }
                                },
                                IoType::WriteAndResponse { response_pid, } => {
                                    self.pid_response_map.insert(response_pid, req);
                                    // TODO: Timeout.
                                },
                                IoType::WriteConnect => {
                                    self.connack_response = Some(req);
                                },
                            }
                        }
                    }
                },
                SelectResult::Ping => {
                    debug!("IoTask: Writing Pingreq");
                    last_write_time = Instant::now();
                    let p = Packet::Pingreq;
                    if let Err(e) = Self::write_packet(&p, &mut stream).await {
                        error!("IoTask: Failed to write ping: {}", e);
                    }
                },
            };
        }
    }

    async fn write_packet(p: &Packet, stream: &mut TcpStream) -> Result<()> {
        trace!("write_packet p={:#?}", p);
        // TODO: Test long packets.
        let mut bytes = BytesMut::with_capacity(MAX_PACKET_SIZE);
        mqttrs::encode(&p, &mut bytes)?;
        trace!("write_packet bytes p={:?}", &*bytes);
        stream.write_all(&*bytes).await?;
        Ok(())
    }

    async fn read_packet(stream: &mut TcpStream) -> Result<Packet> {
        let mut buf = BytesMut::new();
        // TODO: Test long packets.
        buf.resize(MAX_PACKET_SIZE, 0u8);
        let buflen = buf.len();
        let mut n = 0;
        loop {
            let nread = stream.read(&mut buf[n..buflen]).await?;
            n += nread;
            if nread == 0 {
                // Socket disconnected
                error!("IoTask: Socket disconnected");
                return Err(Error::Disconnected);
            }
            trace!("Decoding buf={:?}", &buf[0..n]);
            let decoded = mqttrs::decode(&mut buf)?;
            if let Some(p) = decoded {
                trace!("read_packet p={:#?}", p);
                return Ok(p);
            }
        }
    }
}
