// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go's `net.Conn` as the MySQL server accepts it: a TCP connection from the
//! `host:port` listener or a Unix-domain one from the `socket` listener
//! (`server.go`'s two `startNetworkListener` calls).

use std::io::{self, IoSlice, Read, Write};
use std::net::{Shutdown, TcpStream};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::time::Duration;

/// One accepted client connection.
#[derive(Debug)]
pub enum ClientSocket {
    /// From the TCP listener.
    Tcp(TcpStream),
    /// From the Unix-domain socket listener.
    #[cfg(unix)]
    Unix(UnixStream),
}

impl From<TcpStream> for ClientSocket {
    fn from(stream: TcpStream) -> Self {
        Self::Tcp(stream)
    }
}

#[cfg(unix)]
impl From<UnixStream> for ClientSocket {
    fn from(stream: UnixStream) -> Self {
        Self::Unix(stream)
    }
}

impl ClientSocket {
    /// Another handle on the same connection.
    pub fn try_clone(&self) -> io::Result<Self> {
        match self {
            Self::Tcp(stream) => stream.try_clone().map(Self::Tcp),
            #[cfg(unix)]
            Self::Unix(stream) => stream.try_clone().map(Self::Unix),
        }
    }

    /// Shuts down the read, write, or both halves.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.shutdown(how),
            #[cfg(unix)]
            Self::Unix(stream) => stream.shutdown(how),
        }
    }

    /// Sets the read timeout.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.set_read_timeout(timeout),
            #[cfg(unix)]
            Self::Unix(stream) => stream.set_read_timeout(timeout),
        }
    }

    /// Sets the write timeout.
    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.set_write_timeout(timeout),
            #[cfg(unix)]
            Self::Unix(stream) => stream.set_write_timeout(timeout),
        }
    }

    /// Sets `TCP_NODELAY`; a Unix-domain socket has no Nagle delay.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.set_nodelay(nodelay),
            #[cfg(unix)]
            Self::Unix(_) => Ok(()),
        }
    }

    /// Moves the socket in or out of nonblocking mode.
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.set_nonblocking(nonblocking),
            #[cfg(unix)]
            Self::Unix(stream) => stream.set_nonblocking(nonblocking),
        }
    }


    /// Go `linux.GetSockUID` + `user.LookupId(uid)`: the OS user of the
    /// process on the other end of a Unix-domain socket (`SO_PEERCRED` on
    /// Linux, `LOCAL_PEERCRED` elsewhere, as Go's `sys_linux.go` and
    /// `sys_other.go`).
    pub fn peer_os_user(&self) -> io::Result<String> {
        #[cfg(unix)]
        if let Self::Unix(stream) = self {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            let uid =
                nix::sys::socket::getsockopt(stream, nix::sys::socket::sockopt::PeerCredentials)
                    .map_err(io::Error::from)?
                    .uid();
            #[cfg(not(any(target_os = "linux", target_os = "android")))]
            let uid =
                nix::sys::socket::getsockopt(stream, nix::sys::socket::sockopt::LocalPeerCred)
                    .map_err(io::Error::from)?
                    .uid();
            return nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(uid))
                .map_err(io::Error::from)?
                .map(|user| user.name)
                .ok_or_else(|| io::Error::other(format!("user: unknown userid {uid}")));
        }
        Err(io::Error::other(
            "Expected UNIX socket, but got something else",
        ))
    }

    /// Reads without consuming (`MSG_PEEK`), as `TcpStream::peek`.
    pub fn peek(&self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.peek(buffer),
            #[cfg(unix)]
            Self::Unix(stream) => {
                rustix::net::recv(stream, buffer, rustix::net::RecvFlags::PEEK)
                    .map(|(read, _)| read)
                    .map_err(io::Error::from)
            }
        }
    }
}

impl Read for ClientSocket {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        (&*self).read(buffer)
    }
}

impl Read for &ClientSocket {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            ClientSocket::Tcp(stream) => (&*stream).read(buffer),
            #[cfg(unix)]
            ClientSocket::Unix(stream) => (&*stream).read(buffer),
        }
    }
}

impl Write for ClientSocket {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        (&*self).write(buffer)
    }

    fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
        (&*self).write_vectored(buffers)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self).flush()
    }
}

impl Write for &ClientSocket {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            ClientSocket::Tcp(stream) => (&*stream).write(buffer),
            #[cfg(unix)]
            ClientSocket::Unix(stream) => (&*stream).write(buffer),
        }
    }

    fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
        match self {
            ClientSocket::Tcp(stream) => (&*stream).write_vectored(buffers),
            #[cfg(unix)]
            ClientSocket::Unix(stream) => (&*stream).write_vectored(buffers),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            ClientSocket::Tcp(stream) => (&*stream).flush(),
            #[cfg(unix)]
            ClientSocket::Unix(stream) => (&*stream).flush(),
        }
    }
}
