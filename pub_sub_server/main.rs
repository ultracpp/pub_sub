use tokio::io::{self};

mod buffer_pool;
mod pub_sub_server;

use pub_sub_server::server_run;

#[tokio::main]
async fn main() -> io::Result<()> {
    server_run().await
}
