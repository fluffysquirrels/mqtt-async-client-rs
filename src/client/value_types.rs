pub use mqttrs::{
    QoS,
    SubscribeReturnCodes,
    SubscribeTopic,
};

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

    pub fn topic(&self) -> &str {
        &*self.topic
    }

    pub fn payload(&self) -> &[u8] {
        &*self.payload
    }

    // TODO: More options, especially QoS.
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
