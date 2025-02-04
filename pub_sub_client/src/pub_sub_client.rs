/*
 * rust-pub-sub-server - A high-performance Pub/Sub server built with Rust
 * Copyright (c) 2024 Your Name
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
use tokio::sync::RwLock;
use tokio::io::{self};
use bytes::{BytesMut, BufMut};
use std::sync::Arc;
use std::collections::HashSet;

use crate::tcp_client::TcpClient;

const MAX_PACKET_SIZE: usize = 4096;

pub struct PubSubClient {
    client: TcpClient,
    subscriptions: Arc<RwLock<HashSet<String>>>,
}

impl PubSubClient {
    pub async fn new(addr: &str) -> io::Result<Self> {
        let client = TcpClient::new(addr).await?;
        Ok(PubSubClient {
            client,
            subscriptions: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    pub async fn send_pub_message(&self, topic: &str, message: &str) -> io::Result<()> {
        let msg_type = 0u8;
        let topic_size = topic.len() as u8;
        let message_size = message.len() as u32;
        let body_size = 2 + topic_size as u32 + 4 + message_size;

        let mut buffer = BytesMut::with_capacity(MAX_PACKET_SIZE);
        buffer.put(&body_size.to_be_bytes()[..]);
        buffer.put(&msg_type.to_be_bytes()[..]);
        buffer.put(&topic_size.to_be_bytes()[..]);
        buffer.put(topic.as_bytes());
        buffer.put(&message_size.to_be_bytes()[..]);
        buffer.put(message.as_bytes());

        self.client.send_message(buffer).await
    }

    pub async fn subscribe_to_topic(&self, topic: &str) -> io::Result<()> {
        let subscriptions = self.subscriptions.read().await;
        if subscriptions.contains(topic) {
            return Ok(());
        }
        drop(subscriptions);
        
        let msg_type = 1u8;
        let topic_size = topic.len() as u8;
        let message_size = 0u32;
        let body_size = 2 + topic_size as u32 + 4 + message_size;

        let mut buffer = BytesMut::with_capacity(MAX_PACKET_SIZE);
        buffer.put(&body_size.to_be_bytes()[..]);
        buffer.put(&msg_type.to_be_bytes()[..]);
        buffer.put(&topic_size.to_be_bytes()[..]);
        buffer.put(topic.as_bytes());
        buffer.put(&message_size.to_be_bytes()[..]);

        self.client.send_message(buffer).await?;

        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.insert(topic.to_string());

        Ok(())
    }

    pub async fn unsubscribe_to_topic(&self, topic: &str) -> io::Result<()> {
        let subscriptions = self.subscriptions.read().await;
        if !subscriptions.contains(topic) {
            return Ok(());
        }
        drop(subscriptions);
        
        let msg_type = 2u8;
        let topic_size = topic.len() as u8;
        let message_size = 0u32;
        let body_size = 2 + topic_size as u32 + 4 + message_size;

        let mut buffer = BytesMut::with_capacity(MAX_PACKET_SIZE);
        buffer.put(&body_size.to_be_bytes()[..]);
        buffer.put(&msg_type.to_be_bytes()[..]);
        buffer.put(&topic_size.to_be_bytes()[..]);
        buffer.put(topic.as_bytes());
        buffer.put(&message_size.to_be_bytes()[..]);

        self.client.send_message(buffer).await?;

        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.remove(topic);

        Ok(())
    }

    pub async fn send_request_message(&self, topic: &str, message: &str) -> io::Result<()> {
        let msg_type = 10u8;
        let topic_size = topic.len() as u8;
        let message_size = message.len() as u32;
        let body_size = 2 + topic_size as u32 + 4 + message_size;
    
        let mut buffer = BytesMut::with_capacity(MAX_PACKET_SIZE);
        buffer.put(&body_size.to_be_bytes()[..]);
        buffer.put(&msg_type.to_be_bytes()[..]);
        buffer.put(&topic_size.to_be_bytes()[..]);
        buffer.put(topic.as_bytes());
        buffer.put(&message_size.to_be_bytes()[..]);
        buffer.put(message.as_bytes());
    
        self.client.send_message(buffer).await
    }
    
    pub async fn send_response_message(&self, topic: &str, message: &str) -> io::Result<()> {
        let msg_type = 11u8;
        let topic_size = topic.len() as u8;
        let message_size = message.len() as u32;
        let body_size = 2 + topic_size as u32 + 4 + message_size;
    
        let mut buffer = BytesMut::with_capacity(MAX_PACKET_SIZE);
        buffer.put(&body_size.to_be_bytes()[..]);
        buffer.put(&msg_type.to_be_bytes()[..]);
        buffer.put(&topic_size.to_be_bytes()[..]);
        buffer.put(topic.as_bytes());
        buffer.put(&message_size.to_be_bytes()[..]);
        buffer.put(message.as_bytes());
    
        self.client.send_message(buffer).await
    }

    pub async fn receive_message(&self) -> io::Result<()> {
        let buffer = self.client.receive_message().await?;

        match self.parse_packet(&buffer) {
            Ok((topic, message)) => {
                println!("Received topic: {}, message: {}", topic, message);
                Ok(())
            },
            Err(e) => {
                eprintln!("Failed to parse message: {:?}", e);
                Err(io::Error::new(io::ErrorKind::InvalidData, e))
            }
        }
    }

    fn parse_packet(&self, buffer: &[u8]) -> Result<(String, String), String> {
        let mut cursor = 4;

        let topic_size = *buffer.get(cursor).ok_or("Failed to read topic_size")? as usize;
        cursor += 1;

        let topic = match std::str::from_utf8(&buffer[cursor..cursor + topic_size]) {
            Ok(s) => s.to_string(),
            Err(_) => return Err("Failed to parse topic".to_string()),
        };
        cursor += topic_size;

        let message_size = u32::from_be_bytes(
            buffer[cursor..cursor + 4].try_into().map_err(|_| "Failed to parse message_size")?,
        ) as usize;
        cursor += 4;

        let message = match std::str::from_utf8(&buffer[cursor..cursor + message_size]) {
            Ok(s) => s.to_string(),
            Err(_) => return Err("Failed to parse message".to_string()),
        };

        Ok((topic, message))
    }

    pub async fn close(&self) {
        self.client.close().await;
    }
}
