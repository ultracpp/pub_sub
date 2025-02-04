mod tcp_client;
mod pub_sub_client;

use std::sync::Arc;

use pub_sub_client::PubSubClient;
use tokio::io::{self, AsyncBufReadExt};
use tokio::task;

async fn read_user_input() -> io::Result<String> {
    let mut input = String::new();
    tokio::io::BufReader::new(tokio::io::stdin()).read_line(&mut input).await?;
    Ok(input.trim().to_string())
}

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:55001";
    //let addr = "59.10.146.242:55001";
    let pub_sub_client = match PubSubClient::new(addr).await {
        Ok(client) => Arc::new(client),
        Err(e) => {
            eprintln!("Failed to connect to server: {:?}", e);
            return;
        }
    };

    let pub_sub_client_clone = Arc::clone(&pub_sub_client);
    let receive_task = task::spawn(async move {
        loop {
            if let Err(e) = pub_sub_client_clone.receive_message().await {
                eprintln!("Error receiving message: {:?}", e);
                break;
            }
        }
        pub_sub_client_clone.close().await;
    });

    loop {
        match read_user_input().await {
            Ok(input) if input.eq_ignore_ascii_case("exit") => {
                pub_sub_client.close().await;
                break;
            }
            Ok(input) if input.starts_with("pub ") => {
                let parts: Vec<&str> = input.trim_start_matches("pub ").splitn(2, ' ').collect();
                if parts.len() < 2 {
                    eprintln!("Invalid PUB message format");
                } else {
                    let topic = parts[0].to_string();
                    let message = parts[1].to_string();
                    if let Err(e) = pub_sub_client.send_pub_message(&topic, &message).await {
                        eprintln!("Failed to send PUB message: {:?}", e);
                    }
                }
            }
            Ok(input) if input.starts_with("sub ") => {
                let topic = input.trim_start_matches("sub ").trim().to_string();
                if let Err(e) = pub_sub_client.subscribe_to_topic(&topic).await {
                    eprintln!("Failed to subscribe to topic: {:?}", e);
                }
            }
            Ok(input) if input.starts_with("unsub ") => {
                let topic = input.trim_start_matches("unsub ").trim().to_string();
                if let Err(e) = pub_sub_client.unsubscribe_to_topic(&topic).await {
                    eprintln!("Failed to unsubscribe to topic: {:?}", e);
                }
            }
            Ok(input) if input.starts_with("req ") => {
                let parts: Vec<&str> = input.trim_start_matches("req ").splitn(2, ' ').collect();
                if parts.len() < 2 {
                    eprintln!("Invalid REQ message format");
                } else {
                    let topic = parts[0].to_string();
                    let message = parts[1].to_string();
                    if let Err(e) = pub_sub_client.send_request_message(&topic, &message).await {
                        eprintln!("Failed to send REQ message: {:?}", e);
                    }
                }
            }
            Ok(input) if input.starts_with("res ") => {
                let parts: Vec<&str> = input.trim_start_matches("res ").splitn(2, ' ').collect();
                if parts.len() < 2 {
                    eprintln!("Invalid RES message format");
                } else {
                    let topic = parts[0].to_string();
                    let message = parts[1].to_string();
                    if let Err(e) = pub_sub_client.send_response_message(&topic, &message).await {
                        eprintln!("Failed to send RES message: {:?}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to read input: {:?}", e);
                break;
            }
            _ => {}
        }
    }

    let _ = receive_task.await;
}
