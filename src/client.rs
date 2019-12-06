use bytes::BytesMut;
use crate::{
    Result,
    util::TokioRuntime,
};
use log::{/* error, */ trace};
use mqttrs::{
    ConnectReturnCode,
    Packet,
    Pid,
    QosPid,
    self
};
use tokio::{
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
    net::TcpStream,
};

pub struct Client {
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
    keep_alive: u16,
    _runtime: TokioRuntime,

    stream: Option<TcpStream>,
}

#[derive(Clone, Debug)]
pub struct Publish {
    topic: String,
    payload: Vec<u8>,
}

impl Publish {
    pub fn new(topic: String, payload: Vec<u8>) -> Publish {
        Publish {
            topic,
            payload,
        }
    }

    // TODO: More options, especially QoS.
}

pub use mqttrs::{
    QoS,
    SubscribeReturnCodes,
    SubscribeTopic,
};

#[derive(Debug)]
pub struct Subscribe {
    topics: Vec<SubscribeTopic>,
}

impl Subscribe {
    pub fn new(v: Vec<SubscribeTopic>) -> Subscribe {
        Subscribe {
            topics: v,
        }
    }
}

#[derive(Debug)]
pub struct SubscribeResult {
    return_codes: Vec<SubscribeReturnCodes>,
}

impl SubscribeResult {
    pub fn return_codes(&self) -> &[SubscribeReturnCodes] {
        &*self.return_codes
    }
}

#[derive(Debug)]
pub struct ReadResult {
    topic: String,
    payload: Vec<u8>,
}

impl ReadResult {
    pub fn topic(&self) -> &str {
        &*self.topic
    }

    pub fn payload(&self) -> &[u8] {
        &*self.payload
    }
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub async fn connect(&mut self) -> Result<()> {
        assert!(self.stream.is_none(), "self.stream.is_none()");
        let stream = TcpStream::connect((&*self.host, self.port))
            .await?;
        self.stream = Some(stream);
        let conn = Packet::Connect(mqttrs::Connect {
            protocol: mqttrs::Protocol::MQTT311,
            keep_alive: self.keep_alive,
            client_id: "".to_owned(), // TODO
            clean_session: true, // TODO
            last_will: None, // TODO
            username: self.username.clone(),
            password: self.password.clone().map(|s| s.as_bytes().to_vec()),
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
                self.stream.as_mut().expect("stream").shutdown().await?;
                self.stream = None;
                return Err("Received packet not CONNACK after connect".into());
            }
        }
        // TODO: Spawn a keepalive task.
        Ok(())
    }

    pub async fn publish(&mut self, p: Publish) -> Result<()> {
        let p2 = Packet::Publish(mqttrs::Publish {
            dup: false, // TODO.
            qospid: QosPid::AtMostOnce, // TODO: the other QoS options
            retain: false, // TODO
            topic_name: p.topic,
            payload: p.payload,
        });
        self.write_packet(&p2).await?;
        Ok(())
    }

    pub async fn subscribe(&mut self, s: Subscribe) -> Result<SubscribeResult> {
        let pid = self.alloc_pid()?;
        // TODO: Support subscribe to qos != AtMostOnce.
        if s.topics.iter().any(|t| t.qos != QoS::AtMostOnce) {
            return Err("Only Qos::AtMostOnce supported right now".to_owned().into())
        }
        let p = Packet::Subscribe(mqttrs::Subscribe {
            pid: pid,
            topics: s.topics,
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
        self.stream.as_mut().expect("stream").shutdown().await?;
        self.stream = None;
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
        // TODO: Test long packets and fix the buffer length.
        self.check_connected()?;
        let mut buf = BytesMut::new();
        buf.resize(1024, 0u8);
        let buflen = buf.len();
        let mut n = 0;
        loop {
            let nread = self.stream.as_mut().expect("stream").read(&mut buf[n..buflen]).await?;
            n += nread;
            if nread == 0 {
                // Socket disconnected
                self.stream = None;
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

    // TODO: Send ping requests.
    //  In the absence of sending any other Control Packets, the Client MUST send a PINGREQ Packet
    // If the Keep Alive value is non-zero and the Server does not receive a Control Packet from the Client within one and a half times the Keep Alive time period, it MUST disconnect the Network Connection to the Client as if the network had failed [MQTT-3.1.2-24].
    // If a Client does not receive a PINGRESP Packet within a reasonable amount of time after it has sent a PINGREQ, it SHOULD close the Network Connection to the Server.

    async fn write_packet(&mut self, p: &Packet) -> Result<()> {
        self.check_connected()?;
        trace!("write_packet p={:#?}", p);
        let mut bytes = BytesMut::new();
        mqttrs::encode(&p, &mut bytes)?;
        self.stream.as_mut().expect("stream").write_all(&*bytes).await?;
        Ok(())
    }

    fn check_connected(&mut self) -> Result<()> {
        if self.stream.is_none() {
            // TODO: Return something the consumer can interpret and then reconnect.
            return Err("Not connected".into());
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct ClientBuilder {
    host: Option<String>,
    port: Option<u16>,
    username: Option<String>,
    password: Option<String>,
    keep_alive: Option<u16>,
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
                None => 30,
            },
            _runtime: self.runtime.clone(),

            stream: None,
        })
    }

    /// Required parameter. Set host to connect to.
    pub fn set_host(&mut self, host: String) -> &mut Self {
        self.host = Some(host);
        self
    }

    /// Set TCP port to connect to. The default value is 1883.
    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.port = Some(port);
        self
    }

    /// Set username to authenticate with.
    pub fn set_username(&mut self, username: Option<String>) -> &mut Self {
        self.username = username;
        self
    }

    /// Set password to authenticate with.
    pub fn set_password(&mut self, password: Option<String>) -> &mut Self {
        self.password = password;
        self
    }

    /// Set keep alive time in seconds
    pub fn set_keep_alive(&mut self, keep_alive: u16) -> &mut Self {
        self.keep_alive = Some(keep_alive);
        self
    }

    /// Set the tokio runtime to spawn background tasks onto
    pub fn set_tokio_runtime(&mut self, rt: TokioRuntime) -> &mut Self {
        self.runtime = rt;
        self
    }
}
