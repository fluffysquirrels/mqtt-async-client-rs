pub use mqttrs::{
    QoS,
    SubscribeReturnCodes,
    SubscribeTopic,
};

#[derive(Clone, Debug)]
pub struct Publish {
    topic: String,
    payload: Vec<u8>,
    qos: QoS,
}

impl Publish {
    pub fn new(topic: String, payload: Vec<u8>) -> Publish {
        Publish {
            topic,
            payload,
            qos: QoS::AtMostOnce,
        }
    }

    pub fn topic(&self) -> &str {
        &*self.topic
    }

    pub fn payload(&self) -> &[u8] {
        &*self.payload
    }

    pub fn qos(&self) -> QoS {
        self.qos
    }

    /// Set MQTT quality of service
    pub fn set_qos(&mut self, qos: QoS) -> &mut Self {
        self.qos = qos;
        self
    }
}

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

    pub fn topics(&self) -> &[SubscribeTopic] {
        &*self.topics
    }
}

#[derive(Debug)]
pub struct SubscribeResult {
    pub(crate) return_codes: Vec<SubscribeReturnCodes>,
}

impl SubscribeResult {
    pub fn return_codes(&self) -> &[SubscribeReturnCodes] {
        &*self.return_codes
    }
}

#[derive(Debug)]
pub struct ReadResult {
    pub(crate) topic: String,
    pub(crate) payload: Vec<u8>,
}

impl ReadResult {
    pub fn topic(&self) -> &str {
        &*self.topic
    }

    pub fn payload(&self) -> &[u8] {
        &*self.payload
    }
}

#[derive(Clone, Copy, Debug)]
pub enum KeepAlive {
    Disabled,
    Enabled { secs: u16 },
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
