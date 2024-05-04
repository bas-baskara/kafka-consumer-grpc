# Kafka Consumer with gRPC

This package combines Kafka consumer functionality with gRPC (Remote Procedure Call). The initial design is simple but can grow to meet project requirements as needed.

When using a consumer to listen for events to trigger a call or service, it often requires making adjustments to the subscribed topics or handling incoming messages from specific topics to trigger other services or functions. This process can be inefficient and time-consuming, especially when frequent changes are needed. To address this issue of inefficiency, I created this simple package to streamline the process by providing a flexible and scalable solution for decoupling event handling from service or function calls.

## Features

- Kafka consumer listens for messages on specified topics.
- Messages are processed concurrently and sent to a gRPC server for handling.
- Utilizes goroutines for efficient message processing.
- Gracefully handles shutdown signals for cleanup.

## Usage

### Prerequisites

- Go 1.16 or later installed
- Kafka broker(s) accessible
- gRPC server running

### Installation

```bash
go get -u github.com/bas-baskara/kafka-consumer-grpc
```


## Configuration

Set the following environment variables:

- `KAFKA_BROKER`: Comma-separated list of Kafka broker addresses
- `GRPC_ADDRESS`: Address of gRPC server
- `KAFKA_WORKER_POOL_NUMBER`: Number of worker pools for concurrent message processing
- `GRPC_DIAL_OPTION`: Options for gRPC dialing. Use `insecure` for local development.
- `CERT_FILE_PATH`: Path to the TLS certificate file (required for production environments).



## Running the Consumer

```bash
go run main.go
```

## Gateway

When using this package, it's also recommended to create a gateway between the consumer and the services that will be triggered by the incoming messages. This gateway can act as an intermediary layer, allowing for better control and management of the messages before they reach the services. Additionally, it provides an opportunity to implement additional functionalities such as message validation, transformation, and routing based on business logic requirements.

## License

MIT License



