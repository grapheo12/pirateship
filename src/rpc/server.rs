use std::{fs::File, io::{self, Error}, path, sync::Arc};

use crate::{config::Config, crypto::KeyStore, rpc::auth};
use log::{info, warn};
use rustls::{
    crypto::aws_lc_rs,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use rustls_pemfile::{certs, rsa_private_keys};
use tokio::{
    io::{split, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{rustls, server::TlsStream, TlsAcceptor};

use super::{MessageRef, PinnedMessage, SenderType};

pub struct Server<ServerContext>
where
    ServerContext: Send + Sync + 'static,
{
    pub config: Config,
    pub tls_certs: Vec<CertificateDer<'static>>,
    pub tls_keys: PrivateKeyDer<'static>,
    pub key_store: KeyStore,
    pub msg_handler: fn(&ServerContext, MessageRef) -> Result<Option<PinnedMessage>, Error>, // Can't be a closure as msg_handler is called from another thread.
    do_auth: bool,
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
        handler: fn(&S, MessageRef) -> Result<Option<PinnedMessage>, Error>,
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

    pub fn new_unauthenticated(cfg: &Config, handler: fn(&S, MessageRef) -> Result<Option<PinnedMessage>, Error>) -> Server<S> {
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
                    info!("Authenticated {} at Addr {}", nam, addr);
                    nam
                }
                Err(e) => {
                    warn!("Problem authenticating: {}", e);
                    return Err(e);
                }
            };
            sender = SenderType::Auth(name);
        }
        let (mut rx, mut _tx) = split(stream);
        let mut read_buf = vec![0u8; _server.config.rpc_config.recv_buffer_size as usize];
        loop {
            // Message format: Size(u32) | Message
            // Message size capped at 4GiB.
            // As message is multipart, TCP won't have atomic delivery.
            // It is better to just close connection if that happens.
            // That is why `await?` with all read calls.
            let sz = rx.read_u32().await? as usize;
            if sz == 0 {
                // End of socket, probably?
                // Or can be used as a quit signal.
                break;
            }
            if sz > read_buf.len() {
                let _n = read_buf.len();
                read_buf.reserve(sz - _n);
                info!(
                    "Receive buffer increased capacity to {}",
                    read_buf.capacity()
                );
            }
            let (buf, _) = read_buf.split_at_mut(sz);
            rx.read_exact(buf).await?;

            let resp = (_server.msg_handler)(ctx, MessageRef::from(&read_buf, sz, &sender));
            if let Err(_) = resp {
                break;
            }

            if let Ok(Some(msg)) = resp {
                _tx.write_u32(msg.0.1 as u32).await;
                _tx.write_all(msg.0.0.split_at(msg.0.1).0).await;
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
