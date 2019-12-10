use crate::{
    client::{
        Client,
        KeepAlive,
        ConnectState,
    },
    Result,
    util::TokioRuntime,
};

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
