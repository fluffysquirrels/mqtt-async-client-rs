use crate::{
    client::{
        Client,
        KeepAlive,
        ConnectState,
    },
    Result,
    util::{
        FreePidList,
        TokioRuntime,
    }
};
use std::cell::RefCell;

#[derive(Default)]
pub struct ClientBuilder {
    host: Option<String>,
    port: Option<u16>,
    username: Option<String>,
    password: Option<Vec<u8>>,
    keep_alive: Option<KeepAlive>,
    runtime: TokioRuntime,
    client_id: Option<String>,
    packet_buffer_len: Option<usize>,
    max_packet_len: Option<usize>,
}

impl ClientBuilder {
    pub fn build(&mut self) -> Result<Client> {
        Ok(Client {
            host: match self.host {
                Some(ref h) => h.clone(),
                None => return Err("You must set a host to build a Client".into())
            },
            port: self.port.unwrap_or(1883),
            username: self.username.clone(),
            password: self.password.clone(),
            keep_alive: self.keep_alive.unwrap_or(KeepAlive::from_secs(30)),
            runtime: self.runtime.clone(),
            client_id: self.client_id.clone(),
            packet_buffer_len: self.packet_buffer_len.unwrap_or(100),
            max_packet_len: self.max_packet_len.unwrap_or(64 * 1024),

            state: ConnectState::Disconnected,
            free_write_pids: RefCell::new(FreePidList::new()),
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

    /// Set the ClientId to connect with.
    pub fn set_client_id(&mut self, client_id: Option<String>) -> &mut Self {
        self.client_id = client_id;
        self
    }

    /// Set the inbound and outbound packet buffer length.
    ///
    /// The default is 100.
    pub fn set_packet_buffer_len(&mut self, packet_buffer_len: usize) -> &mut Self {
        self.packet_buffer_len = Some(packet_buffer_len);
        self
    }

    /// Set the maximum packet length.
    ///
    /// The default is 64 * 1024 bytes.
    pub fn set_max_packet_len(&mut self, max_packet_len: usize) -> &mut Self {
        self.max_packet_len = Some(max_packet_len);
        self
    }
}
