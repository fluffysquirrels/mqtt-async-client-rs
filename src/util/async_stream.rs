use futures_util::{stream::Stream, sink::{Sink, SinkExt}};
use tokio::{
    io::{
        AsyncRead,
        AsyncWrite,
        ReadBuf
    },
    net::TcpStream
};
#[cfg(feature = "tls")]
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::{WebSocketStream, MaybeTlsStream};
use tungstenite::{Error, protocol::Message};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

/// A wrapper for the data connection, which may or may not be encrypted.
pub(crate) enum AsyncStream {
    TcpStream(TcpStream),
    #[cfg(feature = "tls")]
    TlsStream(TlsStream<TcpStream>),
    WebSocket(WebSocket),
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
            AsyncStream::WebSocket(socket) => {
                Pin::new(socket).poll_next(cx)
                    .map(|result| {
                         result
                             .unwrap_or(Ok(Message::binary([])))
                             .map(|message| buf.put_slice(&message.into_data()))
                             .map_err(tungstenite_error_to_std_io_error)
                    })
            },
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
            AsyncStream::WebSocket(socket) => {
                let result = match socket.poll_ready_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(v) => v,
                };
                Poll::Ready(match result {
                    Ok(()) => {
                        Pin::new(socket).start_send(Message::binary(buf))
                            .map(|()| buf.len())
                    },
                    Err(e) => Err(e),
                }.map_err(tungstenite_error_to_std_io_error))
            },
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_flush(cx),
            #[cfg(feature = "tls")]
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_flush(cx),
            AsyncStream::WebSocket(socket) => Pin::new(socket).poll_flush(cx).map(|r| r.map_err(tungstenite_error_to_std_io_error)),
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
            AsyncStream::WebSocket(socket) => Pin::new(socket).poll_close(cx).map(|r| r.map_err(tungstenite_error_to_std_io_error)),
        }
    }
}

type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub fn tungstenite_error_to_std_io_error(e: Error) -> io::Error {
    match e {
        Error::Io(e) => e,
        // TODO assign this as appropriate?
        e => io::Error::new(io::ErrorKind::Other, e),
    }
}
