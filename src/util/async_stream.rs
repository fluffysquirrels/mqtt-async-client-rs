use tokio::{io::{AsyncRead, AsyncWrite, ReadBuf}, net::TcpStream};
#[cfg(feature = "tls")]
use tokio_rustls::client::TlsStream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// A wrapper for the data connection, which may or may not be encrypted.
pub(crate) enum AsyncStream {
    TcpStream(TcpStream),
    #[cfg(feature = "tls")]
    TlsStream(TlsStream<TcpStream>),
}

impl AsyncRead for AsyncStream {
    fn poll_read(
        self: Pin<&mut Self>, 
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<std::io::Result<()>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AsyncStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8]
    ) -> Poll<std::result::Result<usize, tokio::io::Error>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_flush(cx),
            #[cfg(feature = "tls")]
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_shutdown(cx),
        }
    }
}
