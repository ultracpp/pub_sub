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
