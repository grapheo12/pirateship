// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use crate::{config::{AtomicConfig, Config}, crypto::{AtomicKeyStore, KeyStore}};
use log::{debug, trace, warn};
use rustls::{crypto::aws_lc_rs, pki_types, RootCertStore};
use std::{
    collections::{HashMap, HashSet}, fs::File, io::{self, BufReader, Cursor, Error, ErrorKind}, ops::{Deref, DerefMut}, path, pin::Pin, sync::{Arc, RwLock}
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
    sync::{
        mpsc::{self, UnboundedSender},
        Mutex,
    },
};
use std::time::Instant;
use tokio_rustls::{client::TlsStream, rustls, TlsConnector};

use super::{auth, server::LatencyProfile, MessageRef, PinnedMessage};

#[derive(Clone)]
pub struct PinnedHashMap<K, V>(Arc<Pin<Box<RwLock<HashMap<K, V>>>>>);
impl<K, V> PinnedHashMap<K, V> {
    fn new() -> PinnedHashMap<K, V> {
        PinnedHashMap(Arc::new(Box::pin(RwLock::new(HashMap::new()))))
    }
}

#[derive(Clone)]
pub struct PinnedHashSet<K>(Arc<Pin<Box<RwLock<HashSet<K>>>>>);
impl<K> PinnedHashSet<K> {
    fn new() -> PinnedHashSet<K> {
        PinnedHashSet(Arc::new(Box::pin(RwLock::new(HashSet::new()))))
    }
}

struct BufferedTlsStream {
    stream: BufWriter<TlsStream<TcpStream>>,
    buffer: Vec<u8>,
    offset: usize,
    bound: usize
}

impl BufferedTlsStream {
    pub fn new(stream: TlsStream<TcpStream>) -> BufferedTlsStream {
        BufferedTlsStream {
            stream: BufWriter::new(stream),
            buffer: vec![0u8; 4096],
            bound: 0,
            offset: 0
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

impl Deref for BufferedTlsStream {
    type Target = BufWriter<TlsStream<TcpStream>>;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl DerefMut for BufferedTlsStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

#[derive(Clone)]
pub struct PinnedTlsStream(Arc<Pin<Box<Mutex<BufferedTlsStream>>>>);
impl PinnedTlsStream {
    fn new(stream: TlsStream<TcpStream>) -> PinnedTlsStream {
        PinnedTlsStream(Arc::new(Box::pin(Mutex::new(BufferedTlsStream::new(
            stream,
        )))))
    }
}
pub struct Client {
    pub config: AtomicConfig,
    pub tls_ca_root_cert: RootCertStore,
    pub sock_map: PinnedHashMap<String, PinnedTlsStream>,
    pub chan_map: PinnedHashMap<String, UnboundedSender<(PinnedMessage, LatencyProfile)>>,
    pub worker_ready: PinnedHashSet<String>,
    pub key_store: AtomicKeyStore,
    do_auth: bool,
}

#[derive(Clone)]
pub struct PinnedClient(pub Arc<Pin<Box<Client>>>);

enum SendDataType<'a> {
    ByteType(MessageRef<'a>),
    SizeType(u32),
}

impl Client {
    fn load_root_ca_cert(path: &String) -> RootCertStore {
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

        let mut root_cert_store = rustls::RootCertStore::empty();
        let mut pem = BufReader::new(f);
        for cert in rustls_pemfile::certs(&mut pem) {
            root_cert_store
                .add(match cert {
                    Ok(_c) => _c,
                    Err(e) => {
                        panic!("Error reading cert: {}", e);
                    }
                })
                .unwrap();
        }
        root_cert_store
    }
    pub fn new(cfg: &Config, key_store: &KeyStore) -> Client {
        Client {
            config: AtomicConfig::new(cfg.clone()),
            tls_ca_root_cert: Client::load_root_ca_cert(&cfg.net_config.tls_root_ca_cert_path),
            sock_map: PinnedHashMap::new(),
            do_auth: true,
            chan_map: PinnedHashMap::new(),
            worker_ready: PinnedHashSet::new(),
            key_store: AtomicKeyStore::new(key_store.to_owned()),
        }
    }
    pub fn new_unauthenticated(cfg: &Config) -> Client {
        Client {
            config: AtomicConfig::new(cfg.clone()),
            tls_ca_root_cert: Client::load_root_ca_cert(&cfg.net_config.tls_root_ca_cert_path),
            sock_map: PinnedHashMap::new(),
            do_auth: false,
            key_store: AtomicKeyStore::new(KeyStore::empty().to_owned()),
            chan_map: PinnedHashMap::new(),
            worker_ready: PinnedHashSet::new(),
        }
    }

    pub fn into(self) -> PinnedClient {
        PinnedClient(Arc::new(Box::pin(self)))
    }
}

impl PinnedClient {
    async fn connect(client: &PinnedClient, name: &String) -> Result<PinnedTlsStream, Error> {
        let cfg = client.0.config.get();
        debug!("Node list: {:?}", cfg.net_config.nodes);
        let peer = cfg
            .net_config
            .nodes
            .get(name)
            .ok_or(ErrorKind::AddrNotAvailable)?;
        // Clones the root cert store. Connect() will be only be called once per node.
        // Or if the connection is dropped and needs to be re-established.
        // So, this should be acceptable.
        let tls_cfg =
            rustls::ClientConfig::builder_with_provider(aws_lc_rs::default_provider().into())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(client.0.tls_ca_root_cert.clone())
                .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(tls_cfg));

        let stream = TcpStream::connect(&peer.addr).await?;
        stream.set_nodelay(true)?;

        let domain = pki_types::ServerName::try_from(peer.domain.as_str())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "invalid dnsname"))?
            .to_owned();

        let mut stream = connector.connect(domain, stream).await?;

        if client.0.do_auth {
            auth::handshake_client(&client, &mut stream).await?;
        }
        let stream_safe = PinnedTlsStream::new(stream.into());
        client
            .0
            .sock_map
            .0
            .write()
            .unwrap()
            .insert(name.to_string(), stream_safe.clone());
        Ok(stream_safe)
    }

    async fn get_sock(client: &PinnedClient, name: &String) -> Result<PinnedTlsStream, Error> {
        // Is there an open connection?
        let mut sock: Option<PinnedTlsStream> = None;
        {
            let sock_map_reader = client.0.sock_map.0.read().unwrap();

            let sock_ = sock_map_reader.get(name);
            if sock_.is_some() {
                sock = Some(sock_.unwrap().clone());
            }
        }

        if sock.is_none() {
            sock = Some(PinnedClient::connect(&client.clone(), name).await?);
        }

        Ok(sock.unwrap())
    }

    async fn send_raw<'a>(
        client: &PinnedClient,
        name: &String,
        sock: &PinnedTlsStream,
        data: SendDataType<'a>,
    ) -> Result<(), Error> {
        let e = match data {
            SendDataType::ByteType(d) => {
                let mut lsock = sock.0.lock().await;
                lsock.write_all(&d).await
            }
            SendDataType::SizeType(d) => {
                let mut lsock = sock.0.lock().await;
                lsock.write_u32(d).await // Does this take care of endianness?
            }
        };

        if let Err(e) = e {
            // There is some problem.
            // Reset connection.
            warn!(
                "Problem sending message to {}: {} ... Resetting connection.",
                name, e
            );
            let mut lsock = sock.0.lock().await;
            if let Err(e2) = lsock.shutdown().await {
                warn!(
                    "Problem shutting down socket of {}: {} ... Proceeding anyway",
                    name, e2
                );
            }

            client.0.sock_map.0.write().unwrap().remove(name);
            debug!("Socket removed from sock_map");
            return Err(e);
        }

        Ok(())
    }
    pub async fn send<'b>(
        client: &PinnedClient,
        name: &String,
        data: MessageRef<'b>,
    ) -> Result<(), Error> {
        let sock = Self::get_sock(client, name).await?;
        let len = data.len() as u32;

        // These two calls will be buffered.
        Self::send_raw(client, name, &sock, SendDataType::SizeType(len)).await?;
        Self::send_raw(client, name, &sock, SendDataType::ByteType(data)).await?;

        {
            // This finally sends the message.
            sock.0.lock().await.flush().await?;
        }

        Ok(())
    }

    pub async fn send_and_await_reply<'b>(
        client: &PinnedClient,
        name: &String,
        data: MessageRef<'b>,
    ) -> Result<PinnedMessage, Error> {
        let sock = Self::get_sock(client, name).await?;
        let len = data.len() as u32;

        let send_time = Instant::now();
        Self::send_raw(client, name, &sock, SendDataType::SizeType(len)).await?;
        let send_sz_time = send_time.elapsed().as_micros();
        Self::send_raw(client, name, &sock, SendDataType::ByteType(data)).await?;
        let send_time = send_time.elapsed().as_micros();

        debug!(
            "Send time: sz:{}, data: {}, total: {} us",
            send_sz_time,
            send_time - send_sz_time,
            send_time
        );

        {
            let mut lsock = sock.0.lock().await;
            lsock.flush().await?;  // Need this flush; otherwise it is a deadlock.

            let mut resp_buf = vec![0u8; 256];
            let sz = lsock.get_next_frame(&mut resp_buf).await? as usize;
            if sz == 0 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "socket probably closed!",
                ));
            }
            
            Ok(PinnedMessage::from(
                resp_buf,
                sz as usize,
                super::SenderType::Auth(name.clone()),
            ))
        }
    }

    pub async fn reliable_send<'b>(
        client: &PinnedClient,
        name: &String,
        data: MessageRef<'b>,
    ) -> Result<(), Error> {
        let cfg = client.0.config.get();
        let mut i = cfg.net_config.client_max_retry;
        while i > 0 {
            let done = match Self::send(client, name, data.clone()).await {
                Ok(()) => true,
                Err(e) => {
                    warn!("Error in reliable send: {}", e);
                    i -= 1;
                    false
                }
            };

            if done {
                return Ok(());
            }
        }

        Err(Error::new(
            ErrorKind::NotConnected,
            "Could not send within max_retries",
        ))
    }

    /// Sends messages to all peers in `names.`
    /// Returns Ok(n) if >= `min_success` sends could be done.
    /// Else returns Err(n)
    /// Each send will be retried for `rpc_config.client_max_retry` consecutively without delay.
    /// Any clever retry mechanism for the broadcast should be implemented by the caller of this function.
    /// (Such as AIMD.)
    pub async fn broadcast(
        client: &PinnedClient,
        names: &Vec<String>,
        data: &PinnedMessage,
        profile: &mut LatencyProfile
    ) -> Result<(), Error> {
        let mut need_to_spawn_workers = Vec::new();
        for name in names {
            let lworkers = client.0.worker_ready.0.read().unwrap();
            if !lworkers.contains(name) {
                need_to_spawn_workers.push(name.clone());
            }
        }

        for name in &need_to_spawn_workers {
            let (tx, mut rx) = mpsc::unbounded_channel(); // (client.0.config.rpc_config.channel_depth as usize);
            let mut lchans = client.0.chan_map.0.write().unwrap();
            lchans.insert(name.clone(), tx);

            let _name = name.clone();
            let _client = client.clone();
            // Register as ready.
            {
                let mut lworkers = _client.0.worker_ready.0.write().unwrap();
                lworkers.insert(_name.clone());
            }
            tokio::spawn(async move {
                // Main loop: Fetch data from channel
                // Do reliable send
                // If reliable send fails, die.

                let mut msgs = Vec::new();
                let c = _client.clone();
                let sock = loop {
                    let s = match Self::get_sock(&c, &_name).await {
                        Ok(s) => s,
                        Err(e) => {
                            debug!("Broadcast worker dying for {}: {}", _name, e);
                            continue;
                        },
                    };

                    break s
                };
                
                while rx.recv_many(&mut msgs, 10).await > 0 {
                    let mut should_print_flush_time = false;
                    let mut combined_prefix = String::from("");
                    let mut should_die = false;
                    for (msg, profile) in &mut msgs {
                        // let instant = msg.1;
                        profile.register("Broadcast chan wait");
                        let msg_ref = msg.as_ref();

                        let len = msg_ref.len() as u32;
                        if let Err(e) = Self::send_raw(&c, &_name, &sock, SendDataType::SizeType(len)).await {
                            warn!("Broadcast worker for {} dying: {}", _name, e);
                            should_die = true;
                            break;
                        }
                        if let Err(e) = Self::send_raw(&c, &_name, &sock, SendDataType::ByteType(msg_ref)).await {
                            warn!("Broadcast worker for {} dying: {}", _name, e);
                            break;
                        }

                        profile.register("Broadcast send raw");

                        profile.prefix += &String::from(format!(" to {} ", _name));
                        if profile.should_print {
                            should_print_flush_time = true;
                            combined_prefix += &profile.prefix.clone();
                        }

                        profile.print();

                    }
                    
                    let flush_time = Instant::now();
                    let _ = sock.0.lock().await.flush().await;

                    if should_print_flush_time {
                        trace!("[{}] Flush time: {} us", combined_prefix, flush_time.elapsed().as_micros());
                    }
                    
                    
                    msgs.clear();

                    if should_die {
                        break;
                    }
                }

                // Deregister as ready.
                {
                    let mut lworkers = _client.0.worker_ready.0.write().unwrap();
                    lworkers.remove(&_name);
                }
            });
        }

        // At this point, all name in names have a dedicated broadcast worker running.
        // let mut chans = Vec::new();
        {
            let lchans = client.0.chan_map.0.read().unwrap();
            for name in names {
                let chan = lchans.get(name).unwrap();
                // chans.push(chan.clone());
                if let Err(e) = chan.send((data.clone(), profile.clone())) {
                    warn!("Broadcast error: {}", e);
                }
            }
        }

        // let _bcast_res = chans.iter().map(|c| c.send((data.clone(), Some(Instant::now()))));

        // join_all(bcast_futs).await;

        Ok(())
    }


    pub async fn drop_connection(client: &PinnedClient, name: &String) {
        let _sock = {
            let mut lsock = client.0.sock_map.0.write().unwrap();
            lsock.remove(name)
        };

        // if sock.is_some() {
        //     let sock = sock.unwrap();
        //     let mut lsock = sock.0.lock().await;
        //     let _ = lsock.shutdown().await;
        // }
    }
}
