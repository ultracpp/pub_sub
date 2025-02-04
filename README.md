# Pub/Sub Server in Rust

## Description

This is a Pub/Sub (Publish/Subscribe) server implemented in Rust using asynchronous programming with `tokio`. The server allows multiple clients to subscribe to topics, publish messages to topics, and handle client requests. It operates in a **Req/Res Model**, where clients communicate indirectly through the server (acting as a message broker). When a client sends a **request** message to the server, the server forwards the request to clients subscribed to the relevant topic, and the responding client sends a **response** back through the server.

The server is designed for **scalability** and **performance**, supporting a large number of concurrent connections and efficiently managing buffers.

## Features

- **Concurrency**: Built with `tokio` for handling multiple client connections concurrently.
- **Configurable**: Server settings such as maximum clients, packet size, and timeouts are configurable.
- **Pub/Sub Model**: Clients can subscribe to topics, unsubscribe, and publish messages to topics.
- **Req/Res Model**: Clients communicate indirectly via the server (broker), where the server acts as a middleman to deliver messages between clients.
- **Buffer Pool**: Efficient buffer management to reduce allocations for each client connection.

## Installation

To get started with this project, follow these steps:

1. Clone the repository:
    ```bash
    git clone https://github.com/ultracpp/pub_sub.git
    cd pub_sub.git
    ```

2. Build and run the project:
    ```bash
    cargo build --release
    cargo run
    ```

3. Configuration:
   Before running the server, you need to configure the server settings in the `Settings.toml` file. See the configuration section below for more details.

## Configuration

The server settings are managed through a `Settings.toml` file. Below are the configuration parameters you can adjust:

```toml
server_addr = "0.0.0.0:55001"  # The address on which the server will listen for incoming client connections.
server_backlog = 128            # The maximum number of pending connections in the server's backlog.

max_clients = 1024             # The maximum number of concurrent clients that the server can handle.
max_packet_size = 4096         # The maximum size (in bytes) of packets that the server can handle.

timeout_wait = 60               # The timeout (in seconds) for waiting on client connections.
timeout_read = 10               # The timeout (in seconds) for reading data from clients.
```

## Usage

Start the server: After building and running the server, it will listen for incoming connections on the address specified in Settings.toml.

Client interaction: Clients can connect to the server, subscribe to topics, and publish messages. Ensure the client sends data in the correct format as expected by the server.

## Contribution
Feel free to fork this project, create issues, and submit pull requests for enhancements or bug fixes. Contributions are welcome!


