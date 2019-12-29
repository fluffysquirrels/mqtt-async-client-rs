use std::{
    convert::From,
    fmt::{Debug, Display, Formatter, self},
};

/// Fallible result values returned by the library.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the library.
#[derive(Debug)]
pub enum Error {
    Disconnected,
    StdError(Box<dyn std::error::Error + Send + Sync>),
    String(String),

    #[doc(hidden)]
    _NonExhaustive
}

impl Error {
    pub fn from_std_err<T: std::error::Error + Send + Sync + 'static>(e: T) -> Error {
        Error::StdError(Box::new(e))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> std::result::Result<(), fmt::Error> {
        match self {
            Error::Disconnected => write!(f, "Disconnected"),
            Error::StdError(e) => write!(f, "{}", e.description()),
            Error::String(s) => write!(f, "{}", s),
            Error::_NonExhaustive => panic!("Not reachable"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::StdError(e) => Some(&**e),
            _ => None,
        }
    }
}

impl From<String> for Error {
    fn from(s: String) -> Error {
        Error::String(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Error {
        Error::String(s.to_owned())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::StdError(Box::new(e))
    }
}

impl From<mqttrs::Error> for Error {
    fn from(e: mqttrs::Error) -> Error {
        Error::StdError(Box::new(e))
    }
}
