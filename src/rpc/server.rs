use std::{collections::HashMap, fs::File, io::{self, Cursor, Error}, path, sync::Arc, time::{Duration, Instant}};

use crate::{config::Config, crypto::KeyStore, rpc::auth};
use indexmap::IndexMap;
use tokio::{io::{BufWriter, ReadHalf}, sync::{mpsc, oneshot}};
use log::{debug, info, trace, warn};
use rustls::{
    crypto::aws_lc_rs,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use rustls_pemfile::{certs, rsa_private_keys};
use tokio::{
    io::{split, AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}
};
use tokio_rustls::{rustls, server::TlsStream, TlsAcceptor};

use super::{MessageRef, PinnedMessage, SenderType};

#[derive(Clone, Debug)]
pub struct LatencyProfile {
    pub start_time: Instant,
    pub should_print: bool,
    pub prefix: String,
    pub durations: IndexMap<String, Duration>
}

impl LatencyProfile {
    pub fn new() -> LatencyProfile {
        LatencyProfile {
            start_time: Instant::now(),
            durations: IndexMap::new(),
            should_print: false,
            prefix: String::from("")
        }
    }

    pub fn register(&mut self, s: &str) {
        let time = self.start_time.elapsed();
        self.durations.insert(s.to_string(), time);
    }

    pub fn print(&self) {
        if !self.should_print {
            return;
        }

        let str_list: Vec<String> = self.durations.iter().map(|(k, v)| {
            format!("{}: {} us", k, v.as_micros())
        }).collect();

        trace!("{}, {}", self.prefix, str_list.join(", "));
    }

    pub fn force_print(&self) {
        let str_list: Vec<String> = self.durations.iter().map(|(k, v)| {
            format!("{}: {} us", k, v.as_micros())
        }).collect();

        info!("{}, {}", self.prefix, str_list.join(", "));
    }
}


pub type MsgAckChan = mpsc::UnboundedSender<(PinnedMessage, LatencyProfile)>;

pub enum RespType {
    Resp = 1,
    NoResp = 2,
    RespAndTrack = 3
}

pub type HandlerType<ServerContext> = fn(
    &ServerContext,             // State kept by upper layers
    MessageRef,                 // New message
    MsgAckChan) -> Result<      // Channel to receive response to message
        RespType,                   // Should the caller wait for a response from the channel?
        Error>;                 // Should this connection be dropped?

pub struct Server<ServerContext>
where
    ServerContext: Send + Sync + 'static,
{
    pub config: Config,
    pub tls_certs: Vec<CertificateDer<'static>>,
    pub tls_keys: PrivateKeyDer<'static>,
    pub key_store: KeyStore,
    pub msg_handler: HandlerType<ServerContext>, // Can't be a closure as msg_handler is called from another thread.
    do_auth: bool,
}

pub struct FrameReader<'a> {
    pub buffer: Vec<u8>,
    pub stream: ReadHalf<&'a mut TlsStream<TcpStream>>,
    pub offset: usize,
    pub bound: usize
}

impl<'a> FrameReader<'a> {
    pub fn new(stream: ReadHalf<&'a mut TlsStream<TcpStream>>) -> FrameReader<'a> {
        FrameReader {
            buffer: vec![0u8; 4096],
            stream,
            offset: 0,
            bound: 0
        }
    }

    async fn read_next_bytes(&mut self, n: usize, v: &mut Vec<u8>) -> io::Result<()> {
        let mut pos = 0;
        let mut n = n;
        let get_n = n;
        while n > 0 {
            if self.bound == self.offset {
                // Need to fetch more data.
                let read_n = self.stream.read(self.buffer.as_mut()).await?;
                debug!("Fetched {} bytes, Need to fetch {} bytes, pos {}, Currently at: {}", read_n, get_n, pos, n);
                self.bound = read_n;
                self.offset = 0;
            }

            if self.bound - self.offset >= n {
                // Copy in full
                v[pos..][..n].copy_from_slice(&self.buffer[self.offset..self.offset+n]);
                self.offset += n;
                n = 0;
            }else{
                // Copy partial.
                // We'll get the rest in the next iteration of the loop.
                v[pos..][..self.bound - self.offset].copy_from_slice(&self.buffer[self.offset..self.bound]);
                n -= self.bound - self.offset;
                pos += self.bound - self.offset;
                self.offset = self.bound;
            }
        }

        Ok(())
        


    }

    pub async fn get_next_frame(&mut self, buff: &mut Vec<u8>) -> io::Result<usize> {
        let mut sz_vec = vec![0u8; 4];
        self.read_next_bytes(4, &mut sz_vec).await?;
        let mut sz_rdr = Cursor::new(sz_vec);
        let sz = sz_rdr.read_u32().await.unwrap() as usize;
        let len = buff.len();
        if sz > len {
            buff.extend(vec![0u8; sz - len]);
            // buff.reserve(sz - len);
        }

        self.read_next_bytes(sz, buff).await?;

        Ok(sz)
    }

}




impl<S> Server<S>
where
    S: Send + Clone + Sync + 'static,
{
    // Following two functions ported from: https://github.com/rustls/tokio-rustls/blob/main/examples/server.rs
    fn load_certs(path: &String) -> Vec<CertificateDer<'static>> {
        let cert_path = path::Path::new(path.as_str());
        if !cert_path.exists() {
            panic!("Invalid Certificate Path: {}", path);
        }
        let f = match File::open(cert_path) {
            Ok(_f) => _f,
            Err(e) => {
                panic!("Problem reading cert file: {}", e);
            }
        };

        match certs(&mut io::BufReader::new(f)).collect() {
            Ok(cert) => cert,
            Err(e) => {
                panic!("Problem parsing cert: {}", e);
            }
        }
    }

    /// This currently reads RSA keys only.
    /// Make sure that the key in path is PKCS1 encoded
    /// ie, begins with "-----BEGIN RSA PRIVATE KEY-----"
    /// Command to do that: openssl rsa -in pkcs8.key -out pkcs1.key
    fn load_keys(path: &String) -> PrivateKeyDer<'static> {
        let key_path = path::Path::new(path.as_str());
        if !key_path.exists() {
            panic!("Invalid Key Path: {}", path);
        }
        let f = match File::open(key_path) {
            Ok(_f) => _f,
            Err(e) => {
                panic!("Problem reading keyfile: {}", e);
            }
        };

        let key_result = rsa_private_keys(&mut io::BufReader::new(f))
            .next()
            .unwrap()
            .map(Into::into);
        match key_result {
            Ok(key) => key,
            Err(e) => {
                panic!("Problem parsing key: {}", e);
            }
        }
    }

    pub fn new(
        cfg: &Config,
        handler: HandlerType<S>,
        key_store: &KeyStore,
    ) -> Server<S> {
        Server {
            config: cfg.clone(),
            tls_certs: Server::<S>::load_certs(&cfg.net_config.tls_cert_path),
            tls_keys: Server::<S>::load_keys(&cfg.net_config.tls_key_path),
            msg_handler: handler,
            do_auth: true,
            key_store: key_store.to_owned(),
        }
    }

    pub fn new_unauthenticated(cfg: &Config, handler: HandlerType<S>) -> Server<S> {
        Server {
            config: cfg.clone(),
            tls_certs: Server::<S>::load_certs(&cfg.net_config.tls_cert_path),
            tls_keys: Server::<S>::load_keys(&cfg.net_config.tls_key_path),
            msg_handler: handler,
            do_auth: false,
            key_store: KeyStore::empty().to_owned(),
        }
    }

    pub async fn handle_stream(
        _server: Arc<Self>,
        ctx: &S,
        stream: &mut TlsStream<TcpStream>,
        addr: core::net::SocketAddr,
    ) -> io::Result<()> {
        let mut sender = SenderType::Anon;
        if _server.do_auth {
            let res = auth::handshake_server(&_server, stream).await;
            let name = match res {
                Ok(nam) => {
                    trace!("Authenticated {} at Addr {}", nam, addr);
                    nam
                }
                Err(e) => {
                    warn!("Problem authenticating: {}", e);
                    return Err(e);
                }
            };
            sender = SenderType::Auth(name);
        }
        let (rx, mut _tx) = split(stream);
        let mut read_buf = vec![0u8; _server.config.rpc_config.recv_buffer_size as usize];
        let mut tx_buf = BufWriter::new(_tx);
        let mut rx_buf = FrameReader::new(rx);
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();
        loop {
            // Message format: Size(u32) | Message
            // Message size capped at 4GiB.
            // As message is multipart, TCP won't have atomic delivery.
            // It is better to just close connection if that happens.
            // That is why `await?` with all read calls.
            let sz = match rx_buf.get_next_frame(&mut read_buf).await {
                Ok(s) => s,
                Err(e) => {
                    debug!("Encountered error while reading frame: {}", e);
                    return Err(e);
                },
            };
            
            // This handler is called from within an async function, although it is not async itself.
            // This is because:
            // 1. I am not nearly good enough in Rust to store an async function pointer in the underlying Server struct
            // 2. This function shouldn't have any blocking code at all. This should be a short running function to send messages to proper channel.
            let resp = (_server.msg_handler)(ctx, MessageRef::from(&read_buf, sz, &sender), ack_tx.clone());
            if let Err(e) = resp {
                warn!("Dropping connection: {}", e);
                break;
            }

            if let Ok(RespType::Resp) = resp {            
                debug!("Waiting for response!");
                let mref = ack_rx.recv().await.unwrap();
                let mref = mref.0.as_ref();
                tx_buf.write_u32(mref.1 as u32).await?;
                tx_buf.write_all(&mref.0[..mref.1]).await?;
                match tx_buf.flush().await {
                    Ok(_) => {},
                    Err(e) => {
                        warn!("Error sending response: {}", e);
                        break;
                    }
                };

            }

            if let Ok(RespType::RespAndTrack) = resp {            
                debug!("Waiting for response!");
                let (mref, mut profile) = ack_rx.recv().await.unwrap();
                profile.register("Ack Received");
                let mref = mref.as_ref();
                tx_buf.write_u32(mref.1 as u32).await?;
                tx_buf.write_all(&mref.0[..mref.1]).await?;
                match tx_buf.flush().await {
                    Ok(_) => {},
                    Err(e) => {
                        warn!("Error sending response: {}", e);
                        break;
                    }
                };


                profile.register("Ack sent");
                profile.print();
            }
        }

        warn!("Dropping connection from {:?}", addr);
        Ok(())
    }
    pub async fn run(server: Arc<Self>, ctx: S) -> io::Result<()> {
        let server_addr = &server.config.net_config.addr;
        info!("Listening on {}", server_addr);

        // aws_lc_rs::default_provider() uses AES-GCM. This automatically includes a MAC.
        // MAC checking is embedded in the TLS messaging, so upper layers don't need to worry.
        let tls_cfg =
            rustls::ServerConfig::builder_with_provider(aws_lc_rs::default_provider().into())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth() // Client and Node auth happen separately after TLS handshake.
                .with_single_cert(server.tls_certs.clone(), server.tls_keys.clone_key())
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

        let tls_acceptor = TlsAcceptor::from(Arc::new(tls_cfg));

        let listener = TcpListener::bind(server_addr).await?;

        loop {
            let (socket, addr) = listener.accept().await?;
            socket.set_nodelay(true)?;
            let acceptor = tls_acceptor.clone();
            let server_ = server.clone();
            let ctx_ = ctx.clone();
            // It is cheap to open a lot of green threads in tokio
            // No need to have a list of sockets to select() from.
            tokio::spawn(async move {
                let mut stream = acceptor.accept(socket).await?;
                Self::handle_stream(server_, &ctx_, &mut stream, addr).await?;
                Ok(()) as io::Result<()>
            });
        }
    }
}
