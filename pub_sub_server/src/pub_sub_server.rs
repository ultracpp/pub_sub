/*
 * rust-pub-sub-server - A high-performance Pub/Sub server built with Rust
 * Copyright (c) 2024 Eungsuk Jeon
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use bytes::{Bytes, BytesMut};
use config::Config;
use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{self, Duration};

use crate::buffer_pool::BufferPool;

extern crate log;
extern crate log4rs;

static CONFIG: Lazy<Config> = Lazy::new(|| {
    Config::builder()
        .add_source(config::File::with_name("Settings"))
        .add_source(config::Environment::with_prefix("APP"))
        .build()
        .unwrap()
});

static SERVER_ADDR: Lazy<String> = Lazy::new(|| CONFIG.get::<String>("server_addr").unwrap_or_default());
static SERVER_BACKLOG: Lazy<u32> = Lazy::new(|| CONFIG.get::<u32>("server_backlog").unwrap_or_default());
static MAX_CLIENTS: Lazy<usize> = Lazy::new(|| CONFIG.get::<usize>("max_clients").unwrap_or_default());
static MAX_PACKET_SIZE: Lazy<usize> = Lazy::new(|| CONFIG.get::<usize>("max_packet_size").unwrap_or_default());

const HEADER_SIZE: usize = 4;
static MAX_BODY_SIZE: Lazy<usize> = Lazy::new(|| { let max_packet_size = *MAX_PACKET_SIZE; max_packet_size - 4 });

static BUFFER_POOL: Lazy<BufferPool> = Lazy::new(|| BufferPool::new(*MAX_PACKET_SIZE, *MAX_CLIENTS));

const TIMEOUT_WAIT: Lazy<Duration> = Lazy::new(|| Duration::from_secs(CONFIG.get::<u64>("timeout_wait").unwrap_or_default()));
const TIMEOUT_READ: Lazy<Duration> = Lazy::new(|| Duration::from_secs(CONFIG.get::<u64>("timeout_read").unwrap_or_default()));

type ClientMap = Arc<RwLock<HashMap<SocketAddr, Arc<ClientStream>>>>;
static CLIENTS: Lazy<ClientMap> = Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

type TopicMap = Arc<RwLock<HashMap<String, Arc<RwLock<HashSet<SocketAddr>>>>>>;
type SubscriptionMap = Arc<RwLock<HashMap<SocketAddr, HashSet<String>>>>;
static TOPICS: Lazy<TopicMap> = Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));
static SUBSCRIPTIONS: Lazy<SubscriptionMap> = Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

type SubjectMap = Arc<RwLock<HashMap<String, SocketAddr>>>;
type RequestMap = Arc<RwLock<HashMap<SocketAddr, String>>>;
static SUBJECT: Lazy<SubjectMap> = Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));
static REQUEST: Lazy<RequestMap> = Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

pub struct ClientStream {
    reader: Arc<Mutex<tokio::io::ReadHalf<TcpStream>>>,
    writer: Arc<Mutex<tokio::io::WriteHalf<TcpStream>>>,
    is_closed: Arc<RwLock<bool>>,
}

impl ClientStream {
    pub fn new(read_half: tokio::io::ReadHalf<TcpStream>, write_half: tokio::io::WriteHalf<TcpStream>) -> Self {
        Self {
            reader: Arc::new(Mutex::new(read_half)),
            writer: Arc::new(Mutex::new(write_half)),
            is_closed: Arc::new(RwLock::new(false)),
        }
    }
}

async fn handle_client(client_stream: Arc<ClientStream>, addr: SocketAddr) -> io::Result<()> {
    let mut buffer = BUFFER_POOL.get_buffer().await;
    
    {
        let mut clients_lock = CLIENTS.write().await;
        clients_lock.insert(addr, client_stream.clone());
        log::info!("Client connected: {}, Total clients: {}", addr, clients_lock.len());
    }

    let result = async {
        loop {
            buffer.clear();
            buffer.resize(HEADER_SIZE, 0);

            let mut reader = client_stream.reader.lock().await;

            match time::timeout(*TIMEOUT_WAIT, reader.read_exact(&mut buffer[..HEADER_SIZE])).await {
                Ok(Ok(_)) => {
                    let body_size = u32::from_be_bytes(buffer[..HEADER_SIZE].try_into().unwrap()) as usize;
                    
                    if body_size > *MAX_BODY_SIZE {
                        return Err(io::Error::new(io::ErrorKind::Other, "Body size exceeds max limit"));
                    }
                    
                    buffer.resize(HEADER_SIZE + body_size, 0);

                    match time::timeout(*TIMEOUT_READ, reader.read_exact(&mut buffer[HEADER_SIZE..])).await {
                        Ok(Ok(_)) => {
                            if let Ok((msg_type, topic, message)) = parse_packet(&buffer) {
                                //println!("{}, {}, {}", msg_type, topic, message);
                                match msg_type {
                                    0 => publish_to_clients(&topic, &message).await,
                                    1 => subscribe_to_topic(&addr, &topic).await,
                                    2 => unsubscribe_to_topic(&addr, &topic).await,
                                    10 => request_topic(&addr, &topic, &message).await,
                                    11 => response_topic(&topic, &message).await,
                                    _ => break,
                                }
                            } else {
                                return Err(io::Error::new(io::ErrorKind::Other, "Invalid packet format"));
                            }
                        }
                        Ok(Err(e)) => return Err(e),
                        Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "Timeout while reading body")),
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "Timeout while reading header")),
            }
        }

        Ok::<(), io::Error>(())
    }
    .await;

    remove_client(&addr).await;
    BUFFER_POOL.return_buffer(buffer).await;

    if let Err(e) = result {
        log::warn!("Client {} error: {}, Total clients: {}", addr, e, CLIENTS.read().await.len());
    } else {
        log::info!("Client disconnected: {}, Total clients: {}", addr, CLIENTS.read().await.len());
    }

    Ok(())
}

fn parse_packet(buffer: &[u8]) -> Result<(u8, String, String), String> {
    let mut cursor = HEADER_SIZE;
    let msg_type = buffer[cursor];
    cursor += 1;
    let topic_size = buffer[cursor] as usize;
    cursor += 1;
    let topic = String::from_utf8_lossy(&buffer[cursor..cursor + topic_size]).to_string();
    cursor += topic_size;
    let message_size = u32::from_be_bytes(buffer[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;
    let message = String::from_utf8_lossy(&buffer[cursor..cursor + message_size]).to_string();
    Ok((msg_type, topic, message))
}

async fn publish_to_clients(topic: &str, message: &str) {
    let clients = {
        let topics_lock = TOPICS.read().await;
        //topics_lock.get(topic).cloned().unwrap_or_else(|| Arc::new(RwLock::new(HashSet::new())))
        topics_lock.get(topic).map(Arc::clone)
    };

    let Some(clients) = clients else { return; };

    let packet = Bytes::from(build_message_packet(topic, message));
    let clients_lock = clients.read().await;

    for client in clients_lock.iter() {
        let packet = packet.clone();

        if let Some(client_stream) = CLIENTS.read().await.get(client) {
            let client_stream = Arc::clone(client_stream);
            
            tokio::spawn(async move {
                let is_closed = *client_stream.is_closed.read().await;
                if is_closed { return; }

                let mut writer = client_stream.writer.lock().await;

                if let Err(e) = writer.write_all(&packet).await {
                    log::warn!("Failed to send message to client: {}", e);
                }
            });
        }
    }
}

fn build_message_packet(topic: &str, message: &str) -> Bytes {
    let topic_bytes = topic.as_bytes();
    let message_bytes = message.as_bytes();
    let topic_size = topic_bytes.len() as u8;
    let message_size = message_bytes.len() as u32;
    let body_size = 1 + topic_size as usize + 4 + message_size as usize;
    let mut buffer = BytesMut::with_capacity(HEADER_SIZE + body_size);
    buffer.extend_from_slice(&(body_size as u32).to_be_bytes());
    buffer.extend_from_slice(&topic_size.to_be_bytes());
    buffer.extend_from_slice(topic_bytes);
    buffer.extend_from_slice(&message_size.to_be_bytes());
    buffer.extend_from_slice(message_bytes);
    buffer.freeze()
}

async fn subscribe_to_topic(addr: &SocketAddr, topic: &str) {
    {
        let mut topics_lock = TOPICS.write().await;
        let clients_set = topics_lock.entry(topic.to_string()).or_insert_with(|| Arc::new(RwLock::new(HashSet::new())));
        clients_set.write().await.insert(*addr);
    }
    
    {
        let mut subscriptions_lock = SUBSCRIPTIONS.write().await;
        let topics_set = subscriptions_lock.entry(*addr).or_insert_with(|| HashSet::new());
        topics_set.insert(topic.to_string());
    }

    log::debug!("Client {} subscribed for topic: {}", addr, topic);
}

async fn unsubscribe_to_topic(addr: &SocketAddr, topic: &str) {
    let mut topics_lock = TOPICS.write().await;

    if let Some(subscribers) = topics_lock.get_mut(topic) {
        let mut subscribers_write = subscribers.write().await;
        subscribers_write.remove(addr);

        let should_remove_topic = subscribers_write.is_empty();

        drop(subscribers_write);

        if should_remove_topic {
            topics_lock.remove(topic);
        }
    }

    let mut subscriptions_lock = SUBSCRIPTIONS.write().await;
    if let Some(topics) = subscriptions_lock.get_mut(addr) {
        topics.remove(topic);

        let should_remove_client = topics.is_empty();

        if should_remove_client {
            subscriptions_lock.remove(addr);
        }
    }

    log::debug!("Client {} unsubscribed from topic: {}", addr, topic);
}

async fn request_topic(addr: &SocketAddr, topic: &str, message: &str) {
    {
        let mut subject_lock = SUBJECT.write().await;
        subject_lock.insert(topic.to_string(), *addr);
    }

    {
        let mut request_lock = REQUEST.write().await;
        request_lock.insert(*addr, topic.to_string());
    }

    publish_to_clients(topic, message).await;
}

async fn response_topic(topic: &str, message: &str) {
    let client_addr = {
        let subject_lock = SUBJECT.read().await;
        subject_lock.get(topic).cloned()
    };

    if let Some(client_addr) = client_addr {
        let clients_read = CLIENTS.read().await;

        if let Some(client_stream) = clients_read.get(&client_addr) {
            let packet = Bytes::from(build_message_packet(topic, message));
            let mut writer = client_stream.writer.lock().await;

            if let Err(e) = writer.write_all(&packet).await {
                log::warn!("Failed to send response to client {}: {}", client_addr, e);
            }
        }
    } else {
        log::warn!("No active request found for topic: {}", topic);
    }
}

async fn remove_client(addr: &SocketAddr) {
    let client_stream = {
        let mut clients_lock = CLIENTS.write().await;
        clients_lock.remove(addr)
    };

    if let Some(client_stream) = client_stream {
        let mut is_closed = client_stream.is_closed.write().await;
        
        if !*is_closed {
            *is_closed = true;
            let mut writer = client_stream.writer.lock().await;
            let _ = writer.shutdown().await;
        }
    }

    {
        let topics = {
            let mut subscriptions_lock = SUBSCRIPTIONS.write().await;
            subscriptions_lock.remove(addr)
        };
    
        if let Some(topics) = topics {
            let mut topics_lock = TOPICS.write().await;
    
            let mut topics_to_remove = Vec::new();
    
            for topic in topics.iter() {
                if let Some(subscribers) = topics_lock.get_mut(topic) {
                    let mut subscribers_write = subscribers.write().await;
                    subscribers_write.remove(addr);
    
                    if subscribers_write.is_empty() {
                        topics_to_remove.push(topic.clone());
                    }
                }
            }
    
            for topic in topics_to_remove {
                topics_lock.remove(&topic);
            }
        }
    }

    {
        let topic = {
            let mut request_lock = REQUEST.write().await;
            request_lock.remove(addr)
        };
    
        if let Some(topic) = topic {
            let mut subject_lock = SUBJECT.write().await;
            subject_lock.remove(&topic);
        }
    }
}

pub async fn server_run() -> io::Result<()> {
    log4rs::init_file("log4rs_rel.yml", Default::default()).unwrap();

    let addr = (&*SERVER_ADDR).parse().unwrap();
    let socket = TcpSocket::new_v4()?; socket.set_reuseaddr(true)?; socket.bind(addr)?;
    let listener = socket.listen(*SERVER_BACKLOG)?;
    log::info!("Listening on {}", addr);

    while let Ok((mut socket, addr)) = listener.accept().await {
        let clients_count = CLIENTS.read().await.len();
        
        if clients_count >= *MAX_CLIENTS {
            log::warn!("Max clients reached ({}). Rejecting connection from {}", *MAX_CLIENTS, addr);
            let _ = socket.shutdown().await;
            continue;
        }

        let (read_half, write_half) = tokio::io::split(socket);
        let client_stream = Arc::new(ClientStream::new(read_half, write_half));
        tokio::spawn(handle_client(client_stream, addr));
    }

    Ok(())
}
