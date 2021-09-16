use crate::{
    client::{
        Client,
        ClientOptions,
        client::ConnectionMode,
        KeepAlive,
    },
    Error, Result,
    util::{
        TokioRuntime,
    }
};

use url::Url;

#[cfg(any(feature = "tls", feature = "websocket"))]
use ::rustls;
#[cfg(feature = "tls")]
use std::sync::Arc;
use tokio::time::Duration;

/// A fluent builder interface to configure a Client.
///
/// Note that you must call `.set_host()` to configure a host to
/// connect to before `.build()`
#[derive(Default)]
pub struct ClientBuilder {
    url: Option<Url>,
    username: Option<String>,
    password: Option<Vec<u8>>,
    keep_alive: Option<KeepAlive>,
    runtime: TokioRuntime,
    client_id: Option<String>,
    packet_buffer_len: Option<usize>,
    max_packet_len: Option<usize>,
    operation_timeout: Option<Duration>,
    connection_mode: ConnectionMode,
    automatic_connect: Option<bool>,
    connect_retry_delay: Option<Duration>,
}

impl ClientBuilder {
    /// Build a new `Client` with this configuration.
    pub fn build(&mut self) -> Result<Client> {
        Client::new(ClientOptions {
            url: self
                .url
                .clone()
                .ok_or(Error::from("You must set a url for the client"))?,
            username: self.username.clone(),
            password: self.password.clone(),
            keep_alive: self.keep_alive.unwrap_or(KeepAlive::from_secs(30)),
            runtime: self.runtime.clone(),
            client_id: self.client_id.clone(),
            packet_buffer_len: self.packet_buffer_len.unwrap_or(100),
            max_packet_len: self.max_packet_len.unwrap_or(64 * 1024),
            operation_timeout: self.operation_timeout.unwrap_or(Duration::from_secs(20)),
            connection_mode: self.connection_mode.clone(),
            automatic_connect: self.automatic_connect.unwrap_or(true),
            connect_retry_delay: self.connect_retry_delay.unwrap_or(Duration::from_secs(30)),
        })
    }

    /// Set the destination url for this mqtt connection to the given string (returning an error if
    /// the provided string is not a valid URL).
    ///
    /// See [Self::set_url] for more details
    pub fn set_url_string(&mut self, url: &str) -> Result<&mut Self> {
        use std::convert::TryFrom;
        let url = Url::try_from(url).map_err(|e| Error::StdError(Box::new(e)))?;
        self.set_url(url)
    }

    /// Set the destination url for this mqtt connection.
    ///
    /// Supported schema are:
    ///   - mqtt: An mqtt session over tcp (default TCP port 1883)
    ///   - mqtts: An mqtt session over tls (default TCP port 8883)
    ///   - ws: An mqtt session over a websocket (default TCP port 80, requires cargo feature "websocket")
    ///   - wss: An mqtt session over a secure websocket (default TCP port 443, requires cargo feature "websocket")
    ///
    /// If the selected scheme is mqtts or wss, then it will preserve the previously provided tls
    /// configuration, if one has been given, or make a new default one otherwise.
    pub fn set_url(&mut self, url: Url) -> Result<&mut Self> {
        #[cfg(any(feature = "tls", feature = "websocket"))]
        let rustls_config = match &self.connection_mode {
            #[cfg(feature = "tls")]
            ConnectionMode::Tls(config) => config.clone(),
            #[cfg(feature = "websocket")]
            ConnectionMode::WebsocketSecure(config) => config.clone(),
            _ => Arc::new(rustls::ClientConfig::new()),
        };
        self.connection_mode = match url.scheme() {
            "mqtt" => ConnectionMode::Tcp,
            #[cfg(feature = "tls")]
            "mqtts" => ConnectionMode::Tls(rustls_config),
            #[cfg(feature = "websocket")]
            "ws" => ConnectionMode::Websocket,
            #[cfg(feature = "websocket")]
            "wss" => ConnectionMode::WebsocketSecure(rustls_config),
            scheme => return Err(Error::String(format!("Unsupported scheme: {}", scheme))),
        };
        self.url = Some(url);
        Ok(self)
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

    /// Set the timeout for operations.
    ///
    /// The default is 20 seconds.
    pub fn set_operation_timeout(&mut self, operation_timeout: Duration) -> &mut Self {
        self.operation_timeout = Some(operation_timeout);
        self
    }

    /// Set the TLS ClientConfig for the client-server connection.
    ///
    /// Enables TLS. By default TLS is disabled.
    #[cfg(feature = "tls")]
    pub fn set_tls_client_config(&mut self, tls_client_config: rustls::ClientConfig) -> &mut Self {
        match self.connection_mode {
            ref mut mode @ ConnectionMode::Tcp => {
                let _ = self.url.as_mut().map(|url| url.set_scheme("mqtts"));
                *mode = ConnectionMode::Tls(Arc::new(tls_client_config))
            }
            ConnectionMode::Tls(ref mut config) => *config = Arc::new(tls_client_config),
            #[cfg(feature = "websocket")]
            ref mut mode @ ConnectionMode::Websocket => {
                *mode = ConnectionMode::WebsocketSecure(Arc::new(tls_client_config))
            }
            #[cfg(feature = "websocket")]
            ConnectionMode::WebsocketSecure(ref mut config) => {
                let _ = self.url.as_mut().map(|url| url.set_scheme("https"));
                *config = Arc::new(tls_client_config)
            }
        }
        self
    }

    #[cfg(feature = "websocket")]
    /// Set the connection to use a websocket
    pub fn set_websocket(&mut self) -> &mut Self {
        self.connection_mode = ConnectionMode::Websocket;
        self
    }

    /// Set whether to automatically connect and reconnect.
    ///
    /// The default is true.
    pub fn set_automatic_connect(&mut self, automatic_connect: bool) -> &mut Self {
        self.automatic_connect = Some(automatic_connect);
        self
    }

    /// Set the delay between connect retries.
    ///
    /// The default is 30s.
    pub fn set_connect_retry_delay(&mut self, connect_retry_delay: Duration) -> &mut Self {
        self.connect_retry_delay = Some(connect_retry_delay);
        self
    }
}
