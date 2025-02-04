use tokio::net::TcpStream;
use tokio::io::{self, AsyncWriteExt, AsyncReadExt};
use bytes::BytesMut;
use std::sync::Arc;
use tokio::sync::{Mutex as TokioMutex, RwLock};
use std::io::ErrorKind;

const HEADER_SIZE: usize = 4;
const MAX_PACKET_SIZE: usize = 4096;
const MAX_BODY_SIZE: usize = 4092;

pub struct TcpClient {
    reader: Arc<TokioMutex<tokio::io::ReadHalf<TcpStream>>>,
    writer: Arc<TokioMutex<tokio::io::WriteHalf<TcpStream>>>,
    is_closed: Arc<RwLock<bool>>,
}

impl TcpClient {
    pub async fn new(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = tokio::io::split(stream);
        Ok(TcpClient {
            reader: Arc::new(TokioMutex::new(reader)),
            writer: Arc::new(TokioMutex::new(writer)),
            is_closed: Arc::new(RwLock::new(false)),
        })
    }

    pub async fn send_message(&self, message: BytesMut) -> io::Result<()> {
        if *self.is_closed.read().await {
            return Err(io::Error::new(ErrorKind::NotConnected, "Connection is closed"));
        }

        let mut writer = self.writer.lock().await;
        writer.write_all(&message).await?;
        writer.flush().await?;
        Ok(())
    }

    pub async fn receive_message(&self) -> io::Result<BytesMut> {
        if *self.is_closed.read().await {
            return Err(io::Error::new(ErrorKind::NotConnected, "Connection is closed"));
        }

        let mut buffer = BytesMut::with_capacity(MAX_PACKET_SIZE);
        buffer.resize(HEADER_SIZE, 0);

        let mut reader = self.reader.lock().await;
        reader.read_exact(&mut buffer).await?;

        let body_size = u32::from_be_bytes(buffer[0..HEADER_SIZE].try_into().unwrap()) as usize;

        if body_size > MAX_BODY_SIZE {
            return Err(io::Error::new(ErrorKind::InvalidData, "Response too large"));
        }

        buffer.resize(HEADER_SIZE + body_size, 0);
        reader.read_exact(&mut buffer[HEADER_SIZE..]).await?;
        Ok(buffer)
    }

    pub async fn close(&self) {
        let mut is_closed = self.is_closed.write().await;
        if *is_closed {
            return;
        }
        *is_closed = true;

        let mut writer = self.writer.lock().await;
        let _ = writer.shutdown().await;
    }
}
