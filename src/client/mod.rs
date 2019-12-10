mod value_types;
pub use value_types::*;

use bytes::BytesMut;
use crate::{
    Result,
    util::TokioRuntime,
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
use std::net::Shutdown;
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

pub struct Client {
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<Vec<u8>>,
    keep_alive: KeepAlive,
    runtime: TokioRuntime,

    state: ConnectState,
}

enum ConnectState {
    Disconnected,
    Connected(ClientConnection)
}

struct ClientConnection {
    tx_to_send: mpsc::Sender<IoRequest>,
    rx_to_recv: mpsc::Receiver<Packet>,
}

struct IoTask {
    keep_alive: KeepAlive,

    stream: TcpStream,
    rx_to_send: mpsc::Receiver<IoRequest>,
    tx_to_recv: mpsc::Sender<Packet>,

    /// The time the last packet was written to `stream`.
    /// Used to calculate when to send a Pingreq
    last_write_time: Instant,
}

#[derive(Debug)]
struct IoRequest {
    packet: Packet,
    tx_result: oneshot::Sender<IoResult>,
}

#[derive(Debug)]
struct IoResult {
    result: Result<()>,
}

#[derive(Clone, Copy, Debug)]
pub enum KeepAlive {
    Disabled,
    Enabled { secs: u16 },
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub async fn connect(&mut self) -> Result<()> {
        self.check_disconnected()?;
        let stream = TcpStream::connect((&*self.host, self.port))
            .await?;
        let (tx_to_send, rx_to_send) = mpsc::channel::<IoRequest>(100);
        // TODO: Change this to allow control messages, e.g. disconnected?
        let (tx_to_recv, rx_to_recv) = mpsc::channel::<Packet>(100);
        self.state = ConnectState::Connected(ClientConnection {
            tx_to_send,
            rx_to_recv,
        });
        let io = IoTask {
            keep_alive: self.keep_alive,
            stream,
            rx_to_send,
            tx_to_recv,
            last_write_time: Instant::now(),
        };
        self.runtime.spawn(io.run());

        let conn = Packet::Connect(mqttrs::Connect {
            protocol: mqttrs::Protocol::MQTT311,
            keep_alive: match self.keep_alive {
                KeepAlive::Disabled => 0,
                KeepAlive::Enabled { secs } => secs,
            },
            client_id: "".to_owned(), // TODO
            clean_session: true, // TODO
            last_will: None, // TODO
            username: self.username.clone(),
            password: self.password.clone(),
        });
        self.write_packet(&conn).await?;
        // TODO: timeout on CONNACK
        let connack = self.read_packet().await?;
        match connack {
            Packet::Connack(ca) => {
                if ca.code != ConnectReturnCode::Accepted {
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
        let p2 = Packet::Publish(mqttrs::Publish {
            dup: false, // TODO.
            qospid: QosPid::AtMostOnce, // TODO: the other QoS options
            retain: false, // TODO
            topic_name: p.topic().to_owned(),
            payload: p.payload().to_owned(),
        });
        self.write_packet(&p2).await?;
        Ok(())
    }

    pub async fn subscribe(&mut self, s: Subscribe) -> Result<SubscribeResult> {
        let pid = self.alloc_pid()?;
        // TODO: Support subscribe to qos != AtMostOnce.
        if s.topics().iter().any(|t| t.qos != QoS::AtMostOnce) {
            return Err("Only Qos::AtMostOnce supported right now".to_owned().into())
        }
        let p = Packet::Subscribe(mqttrs::Subscribe {
            pid: pid,
            topics: s.topics().to_owned(),
        });
        self.write_packet(&p).await?;
        let r = self.read_packet().await?;
        // TODO: Implement timeout.
        // TODO: Handle other packets.
        match r {
            Packet::Suback(mqttrs::Suback {
                pid: suback_pid,
                return_codes: rcs,
            }) if suback_pid == pid => {
                self.free_pid(pid)?;
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

    pub async fn read(&mut self) -> Result<ReadResult> {
        let r = self.read_packet().await?;
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

    pub async fn disconnect(&mut self) -> Result<()> {
        self.check_connected()?;
        let p = Packet::Disconnect;
        self.write_packet(&p).await?;
        self.shutdown().await?;
        Ok(())
    }

    fn alloc_pid(&mut self) -> Result<Pid> {
        // TODO: Track used Pid's
        Ok(Pid::try_from(1).expect("Non-zero Pid"))
    }

    fn free_pid(&mut self, _p: Pid) -> Result<()> {
        // TODO: Track used Pid's
        Ok(())
    }

    async fn read_packet(&mut self) -> Result<Packet> {
        let c = self.check_connected()?;
        match c.rx_to_recv.recv().await {
            Some(p) => Ok(p),
            None => {
                // Sender closed.
                self.state = ConnectState::Disconnected;
                Err("IoTask sender closed".into())
            }
        }
    }

    async fn shutdown(&mut self) -> Result <()> {
        let _c = self.check_connected()?;
        // Setting the state drops the write end of the channel to the IoTask, which
        // will then shut down the stream.
        self.state = ConnectState::Disconnected;
        Ok(())
    }

    async fn write_packet(&mut self, p: &Packet) -> Result<()> {
        let c = self.check_connected()?;
        let (tx, rx) = oneshot::channel::<IoResult>();
        let req = IoRequest {
            packet: p.clone(),
            tx_result: tx,
        };
        c.tx_to_send.send(req).await?;
        // TODO: Add a timeout?
        let res = rx.await?;
        res.result
    }

    fn check_connected(&mut self) -> Result<&mut ClientConnection> {
        match self.state {
            // TODO: Return something the consumer can interpret and then reconnect.
            ConnectState::Disconnected => Err("Not connected".into()),
            ConnectState::Connected(ref mut c) => Ok(c),
        }
    }

    fn check_disconnected(&mut self) -> Result<()> {
        match self.state {
            // TODO: Return something the consumer can interpret and then reconnect.
            ConnectState::Disconnected => Ok(()),
            ConnectState::Connected(_) => Err("Connected already".into()),
        }
    }
}

enum SelectResult {
    Req(Option<IoRequest>),
    Read(Result<Packet>),
    Ping,
}

impl IoTask {
    async fn run(mut self) {
        let IoTask { mut rx_to_send, mut stream, mut last_write_time, .. } = self;
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
                let mut req_fut = Box::pin(rx_to_send.recv().fuse());
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
                         Err(e) => {
                             // TODO: Handle disconnect
                             error!("IoTask: Failed to read packet: {}", e);
                         },
                         Ok(p) => {
                             if let Packet::Pingresp = p {
                                 debug!("IoTask: Ignoring Pingresp");
                                 continue
                             }
                             if let Err(e) = self.tx_to_recv.send(p).await {
                                 error!("IoTask: Failed to send Packet: {}", e);
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
                        let res = IoResult { result: res };
                        if let Err(_) = req.tx_result.send(res) {
                            error!("IoTask: Failed to send IoResult");
                        }
                    }
                },
                SelectResult::Ping => {
                    // TODO: If a Client does not receive a
                    // PINGRESP Packet within a reasonable amount
                    // of time after it has sent a PINGREQ, it
                    // SHOULD close the Network Connection to the
                    // Server.

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
        let mut bytes = BytesMut::new();
        mqttrs::encode(&p, &mut bytes)?;
        stream.write_all(&*bytes).await?;
        Ok(())
    }

    async fn read_packet(stream: &mut TcpStream) -> Result<Packet> {
        let mut buf = BytesMut::new();
        // TODO: Test long packets and fix the buffer length.
        buf.resize(1024, 0u8);
        let buflen = buf.len();
        let mut n = 0;
        loop {
            let nread = stream.read(&mut buf[n..buflen]).await?;
            n += nread;
            if nread == 0 {
                // Socket disconnected
                return Err("TcpStream disconnected".into());
            }
            trace!("Decoding buf={:?}", &buf[0..n]);
            let decoded = mqttrs::decode(&mut buf)?;
            if let Some(p) = decoded {
                trace!("read_packet p={:#?}", p);
                // TODO: Handle ping responses.
                return Ok(p);
            }
        }
    }
}

#[derive(Default)]
pub struct ClientBuilder {
    host: Option<String>,
    port: Option<u16>,
    username: Option<String>,
    password: Option<Vec<u8>>,
    keep_alive: Option<KeepAlive>,
    runtime: TokioRuntime,
}

impl ClientBuilder {
    pub fn build(&mut self) -> Result<Client> {
        Ok(Client {
            host: match self.host {
                Some(ref h) => h.clone(),
                None => return Err("You must set a host to build a Client".into())
            },
            port: match self.port {
                Some(p) => p,
                None => 1883,
            },
            username: self.username.clone(),
            password: self.password.clone(),
            keep_alive: match self.keep_alive {
                Some(k) => k,
                None => KeepAlive::from_secs(30),
            },
            runtime: self.runtime.clone(),

            state: ConnectState::Disconnected,
        })
    }

    /// Set host to connect to. This is a required parameter.
    pub fn set_host(&mut self, host: String) -> &mut Self {
        self.host = Some(host);
        self
    }

    /// Set TCP port to connect to.
    ///
    /// The default value is 1883.
    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.port = Some(port);
        self
    }

    /// Set username to authenticate with.
    ///
    /// The default value is no username.
    pub fn set_username(&mut self, username: Option<String>) -> &mut Self {
        self.username = username;
        self
    }

    /// Set password to authenticate with.
    ///
    /// The default is no password.
    pub fn set_password(&mut self, password: Option<Vec<u8>>) -> &mut Self {
        self.password = password;
        self
    }

    /// Set keep alive time.
    ///
    /// This controls how often ping requests are sent when the connection is idle.
    /// See [MQTT 3.1.1 specification section 3.1.2.10](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/csprd02/mqtt-v3.1.1-csprd02.html#_Keep_Alive)
    ///
    /// The default value is 30 seconds.
    pub fn set_keep_alive(&mut self, keep_alive: KeepAlive) -> &mut Self {
        self.keep_alive = Some(keep_alive);
        self
    }

    /// Set the tokio runtime to spawn background tasks onto.
    ///
    /// The default is to use the default tokio runtime, i.e. `tokio::spawn()`.
    pub fn set_tokio_runtime(&mut self, rt: TokioRuntime) -> &mut Self {
        self.runtime = rt;
        self
    }
}

impl KeepAlive {
    /// Set keep alive time in seconds.
    ///
    /// Panics if `secs` parameter is 0.
    pub fn from_secs(secs: u16) -> KeepAlive {
        if secs == 0 {
            panic!("KeepAlive secs == 0 not permitted");
        }
        KeepAlive::Enabled { secs, }
    }

    /// Disable keep alive functionality.
    pub fn disabled() -> KeepAlive {
        KeepAlive::Disabled
    }
}
