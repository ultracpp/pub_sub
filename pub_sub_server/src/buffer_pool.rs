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
use tokio::sync::Mutex;
use bytes::BytesMut;

pub struct BufferPool {
    max_packet_size: usize,
    max_clients: usize,
    pool: Mutex<Vec<BytesMut>>,
}

impl BufferPool {
    pub fn new(max_packet_size: usize, max_clients: usize) -> Self {
        Self {
            max_packet_size,
            max_clients,
            pool: Mutex::new(Vec::with_capacity(max_clients)),
        }
    }

    pub async fn get_buffer(&self) -> BytesMut {
        let mut pool = self.pool.lock().await;
        pool.pop().unwrap_or_else(|| BytesMut::with_capacity(self.max_packet_size))
    }

    pub async fn return_buffer(&self, mut buffer: BytesMut) {
        buffer.clear();
        let mut pool = self.pool.lock().await;
        if pool.len() < self.max_clients {
            pool.push(buffer);
        }
    }
}
