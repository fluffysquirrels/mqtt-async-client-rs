use crate::Result;
use mqttrs::{
    QoS,
    SubscribeReturnCodes,
    SubscribeTopic,
};

/// Arguments for a publish operation.
#[derive(Clone, Debug)]
pub struct Publish {
    topic: String,
    payload: Vec<u8>,
    qos: QoS,
}

impl Publish {
    /// Construct a new instance.
    pub fn new(topic: String, payload: Vec<u8>) -> Publish {
        Publish {
            topic,
            payload,
            qos: QoS::AtMostOnce,
        }
    }

    /// Returns the topic name of this instance.
    pub fn topic(&self) -> &str {
        &*self.topic
    }

    /// Returns the payload data of this instance.
    pub fn payload(&self) -> &[u8] {
        &*self.payload
    }

    /// Returns the QoS level configured.
    pub fn qos(&self) -> QoS {
        self.qos
    }

    /// Set MQTT quality of service.
    ///
    /// The default is QoS::AtMostOnce.
    pub fn set_qos(&mut self, qos: QoS) -> &mut Self {
        self.qos = qos;
        self
    }
}

/// Arguments for a subscribe operation.
#[derive(Debug)]
pub struct Subscribe {
    topics: Vec<SubscribeTopic>,
}

impl Subscribe {
    /// Construct a new instance.
    pub fn new(v: Vec<SubscribeTopic>) -> Subscribe {
        Subscribe {
            topics: v,
        }
    }

    /// Returns the topics selected.
    pub fn topics(&self) -> &[SubscribeTopic] {
        &*self.topics
    }
}

/// The return value from a subscribe operation.
#[derive(Debug)]
pub struct SubscribeResult {
    pub(crate) return_codes: Vec<SubscribeReturnCodes>,
}

impl SubscribeResult {
    /// Returns the return codes from the operation.
    pub fn return_codes(&self) -> &[SubscribeReturnCodes] {
        &*self.return_codes
    }

    /// Returns an error if any return codes from the operation were `Failure`.
    pub fn any_failures(&self) -> Result<()> {
        let any_failed =
            self.return_codes().iter()
                .any(|rc| *rc == SubscribeReturnCodes::Failure);
        if any_failed {
            return Err(format!("Some subscribes failed: {:#?}", self.return_codes()).into());
        }
        Ok(())
    }
}

/// Arguments for an unsubscribe operation.
pub struct Unsubscribe {
    topics: Vec<UnsubscribeTopic>
}

impl Unsubscribe {
    /// Construct a new instance.
    pub fn new(topics: Vec<UnsubscribeTopic>) -> Unsubscribe {
        Unsubscribe { topics: topics }
    }

    /// Returns the topics for the operation
    pub fn topics(&self) -> &[UnsubscribeTopic] {
        &*self.topics
    }
}

/// A topic for an unsubscribe operation.
pub struct UnsubscribeTopic {
    topic_name: String,
}

impl UnsubscribeTopic {
    /// Construct a new instance.
    pub fn new(topic_name: String) -> UnsubscribeTopic {
        UnsubscribeTopic { topic_name: topic_name }
    }

    /// Returns the topic name for the operation.
    pub fn topic_name(&self) -> &str {
        &*self.topic_name
    }
}

/// The result from a read subscriptions operation.
#[derive(Debug)]
pub struct ReadResult {
    pub(crate) topic: String,
    pub(crate) payload: Vec<u8>,
}

impl ReadResult {
    /// Returns the topic that was published to.
    pub fn topic(&self) -> &str {
        &*self.topic
    }

    /// Returns the payload data that was published.
    pub fn payload(&self) -> &[u8] {
        &*self.payload
    }
}

/// Represents the keep alive setting for a client.
#[derive(Clone, Copy, Debug)]
pub enum KeepAlive {
    /// Keep alive ping packets are disabled.
    Disabled,

    /// Send a keep alive ping packet every `secs` seconds.
    Enabled {
        /// The number of seconds between packets.
        secs: u16
    },
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
