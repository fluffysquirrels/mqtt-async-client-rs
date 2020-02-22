use crate::Result;
use mqttrs::{QoS, SubscribeReturnCodes, SubscribeTopic};
use std::borrow::Cow;
use tokio::time::Duration;

const BUF_SIZE: usize = 8;

/// A publish payload.
#[derive(Clone, Debug)]
pub enum Payload {
    Fixed([u8; BUF_SIZE], u8),
    Variable(Vec<u8>),
}

impl From<Vec<u8>> for Payload {
    fn from(data: Vec<u8>) -> Self {
        Self::Variable(data)
    }
}
impl From<&[u8; 1]> for Payload {
    fn from(data: &[u8; 1]) -> Self {
        Self::Fixed([data[0]; BUF_SIZE], 1)
    }
}
impl From<[u8; 1]> for Payload {
    fn from(data: [u8; 1]) -> Self {
        (&data).into()
    }
}
impl From<&[u8; 2]> for Payload {
    fn from(data: &[u8; 2]) -> Self {
        let mut buf = [0_u8; BUF_SIZE];
        buf[..data.len()].copy_from_slice(data);
        Self::Fixed(buf, data.len() as u8)
    }
}
impl From<[u8; 2]> for Payload {
    fn from(data: [u8; 2]) -> Self {
        (&data).into()
    }
}
impl From<&[u8; 3]> for Payload {
    fn from(data: &[u8; 3]) -> Self {
        let mut buf = [0_u8; BUF_SIZE];
        buf[..data.len()].copy_from_slice(data);
        Self::Fixed(buf, data.len() as u8)
    }
}
impl From<[u8; 3]> for Payload {
    fn from(data: [u8; 3]) -> Self {
        (&data).into()
    }
}
impl From<&[u8; 4]> for Payload {
    fn from(data: &[u8; 4]) -> Self {
        let mut buf = [0_u8; BUF_SIZE];
        buf[..data.len()].copy_from_slice(data);
        Self::Fixed(buf, data.len() as u8)
    }
}
impl From<[u8; 4]> for Payload {
    fn from(data: [u8; 4]) -> Self {
        (&data).into()
    }
}
impl From<&[u8; 5]> for Payload {
    fn from(data: &[u8; 5]) -> Self {
        let mut buf = [0_u8; BUF_SIZE];
        buf[..data.len()].copy_from_slice(data);
        Self::Fixed(buf, data.len() as u8)
    }
}
impl From<[u8; 5]> for Payload {
    fn from(data: [u8; 5]) -> Self {
        (&data).into()
    }
}
impl From<&[u8; 6]> for Payload {
    fn from(data: &[u8; 6]) -> Self {
        let mut buf = [0_u8; BUF_SIZE];
        buf[..data.len()].copy_from_slice(data);
        Self::Fixed(buf, data.len() as u8)
    }
}
impl From<[u8; 6]> for Payload {
    fn from(data: [u8; 6]) -> Self {
        (&data).into()
    }
}
impl From<&[u8; 7]> for Payload {
    fn from(data: &[u8; 7]) -> Self {
        let mut buf = [0_u8; BUF_SIZE];
        buf[..data.len()].copy_from_slice(data);
        Self::Fixed(buf, data.len() as u8)
    }
}
impl From<[u8; 7]> for Payload {
    fn from(data: [u8; 7]) -> Self {
        (&data).into()
    }
}
impl From<&[u8; 8]> for Payload {
    fn from(data: &[u8; 8]) -> Self {
        let mut buf = [0_u8; BUF_SIZE];
        buf[..data.len()].copy_from_slice(data);
        Self::Fixed(buf, data.len() as u8)
    }
}
impl From<[u8; 8]> for Payload {
    fn from(data: [u8; 8]) -> Self {
        (&data).into()
    }
}

impl From<u8> for Payload {
    fn from(data: u8) -> Self {
        [data; 1].into()
    }
}
impl From<i8> for Payload {
    fn from(data: i8) -> Self {
        [data as u8; 1].into()
    }
}
impl From<u16> for Payload {
    fn from(data: u16) -> Self {
        data.to_le_bytes().into()
    }
}
impl From<i16> for Payload {
    fn from(data: i16) -> Self {
        data.to_le_bytes().into()
    }
}
impl From<u32> for Payload {
    fn from(data: u32) -> Self {
        data.to_le_bytes().into()
    }
}
impl From<i32> for Payload {
    fn from(data: i32) -> Self {
        data.to_le_bytes().into()
    }
}
impl From<u64> for Payload {
    fn from(data: u64) -> Self {
        data.to_le_bytes().into()
    }
}
impl From<i64> for Payload {
    fn from(data: i64) -> Self {
        data.to_le_bytes().into()
    }
}
impl From<f32> for Payload {
    fn from(data: f32) -> Self {
        data.to_le_bytes().into()
    }
}
impl From<f64> for Payload {
    fn from(data: f64) -> Self {
        data.to_le_bytes().into()
    }
}

/// Arguments for a publish operation.
#[derive(Clone, Debug)]
pub struct Publish {
    topic: Cow<'static, str>,
    payload: Payload,
    qos: QoS,
    retain: bool,
}

impl Publish {
    /// Construct a new instance.
    pub fn new<T, V>(topic: T, payload: V) -> Publish
    where
        T: Into<Cow<'static, str>>,
        V: Into<Payload>,
    {
        Publish::new_with_qos(topic, payload, QoS::AtMostOnce)
    }

    /// Construct a new instance with QoS.
    pub fn new_with_qos<T, V>(topic: T, payload: V, qos: QoS) -> Publish
    where
        T: Into<Cow<'static, str>>,
        V: Into<Payload>,
    {
        Publish {
            topic: topic.into(),
            payload: payload.into(),
            qos: qos,
            retain: false,
        }
    }

    /// Returns the topic name of this instance.
    pub fn topic(&self) -> &str {
        &*self.topic
    }

    /// Returns the payload data of this instance.
    pub fn payload(&self) -> &[u8] {
        match self.payload {
            Payload::Fixed(ref buf, len) => &buf[..(len as usize)],
            Payload::Variable(ref buf) => buf,
        }
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

    /// Set value of the retain flag.
    ///
    /// The default is false.
    ///
    /// See MQTT 3.1.1 section 3.3.1.3 http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html#_Toc385349265
    pub fn set_retain(&mut self, retain: bool) -> &mut Self {
        self.retain = retain;
        self
    }

    /// Returns the retain flag value configured.
    pub fn retain(&self) -> bool {
        self.retain
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
        Subscribe { topics: v }
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
        let any_failed = self
            .return_codes()
            .iter()
            .any(|rc| *rc == SubscribeReturnCodes::Failure);
        if any_failed {
            return Err(format!("Some subscribes failed: {:#?}", self.return_codes()).into());
        }
        Ok(())
    }
}

/// Arguments for an unsubscribe operation.
pub struct Unsubscribe {
    topics: Vec<UnsubscribeTopic>,
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
        UnsubscribeTopic {
            topic_name: topic_name,
        }
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
        secs: u16,
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
        KeepAlive::Enabled { secs }
    }

    /// Disable keep alive functionality.
    pub fn disabled() -> KeepAlive {
        KeepAlive::Disabled
    }

    /// Returns whether keep alives are enabled.
    pub fn is_enabled(&self) -> bool {
        match self {
            KeepAlive::Disabled => false,
            KeepAlive::Enabled { .. } => true,
        }
    }

    /// Returns whether keep alives are disabled.
    pub fn is_disabled(&self) -> bool {
        match self {
            KeepAlive::Disabled => true,
            KeepAlive::Enabled { .. } => false,
        }
    }

    /// Returns the keep alive interval if enabled as Some(tokio::Duration),
    /// or None if disabled.
    pub fn as_duration(&self) -> Option<Duration> {
        match self {
            KeepAlive::Disabled => None,
            KeepAlive::Enabled { secs } => Some(Duration::from_secs(*secs as u64)),
        }
    }
}
