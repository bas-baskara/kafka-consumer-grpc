package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
	"github.com/bas-baskara/kafka-consumer-grpc/configs"
	pb "github.com/bas-baskara/kafka-consumer-grpc/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var env = configs.EnvGetter()

func initClientGrpc() (*pb.ListenMessagesClient, *grpc.ClientConn, error) {
	var opts []grpc.DialOption

	if env.GRPC_DIAL_OPTION == "insecure" {
		opts = append(opts, grpc.WithInsecure())
	} else {
		creds, err := credentials.NewClientTLSFromFile(env.CERT_FILE_PATH, "")
		if err != nil {
			log.Fatalf("Failed to create TLS credentials: %v", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}

	conn, err := grpc.Dial(env.GRPC_ADDRESS, opts...)
	if err != nil {
		log.Fatal("Failed to dial server:", err)
		return nil, nil, err
	}

	client := pb.NewListenMessagesClient(conn)

	log.Println("grpc connection was started...")

	return &client, conn, nil

}

func main() {
	// initialize grpc client
	client, conn, err := initClientGrpc()
	if err != nil {
		log.Fatalf("Failed to initialize gRPC client: %v", err)
	}

	defer conn.Close()

	// kafka broker addresses
	brokers := strings.Split(env.KAFKA_BROKER, ",")

	// Create configuration
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create consumer
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Println("Error closing kafka consumer:", err)
		}
	}()

	// subscribe to all topics
	topics, err := consumer.Topics()
	if err != nil {
		log.Println("Error getting topics:", err)
		return
	}

	// Trap SIGINT to trigger a graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// create channel
	messageChan := make(chan *sarama.ConsumerMessage)

	workerNumber, err := strconv.Atoi(env.KAFKA_WORKER_POOL_NUMBER)
	if err != nil {
		log.Fatal("Worker number not set:", err)
		return
	}

	// start worker

	for i := 0; i < workerNumber; i++ {
		go worker(messageChan, *client)
	}

	// consume messages
	consumeMessages(topics, consumer, messageChan, signals)

	// wait for shutdown signal
	<-signals
	log.Println("Shutdown signal received. Exiting...")

}

func worker(messages <-chan *sarama.ConsumerMessage, client pb.ListenMessagesClient) {
	ctx := context.Background()
	for msg := range messages {

		timestamp := timestamppb.New(msg.Timestamp)

		res, err := client.HandleIncomingMessage(ctx, &pb.IncomingMessage{
			Topic:     msg.Topic,
			Value:     msg.Value,
			Timestamp: timestamp,
			Key:       string(msg.Key),
		})

		if err != nil {
			log.Println("error when client handle incoming message", err)
			continue
		}

		log.Printf("client successfully handle incoming message: %v", res)

	}
}

func consumeMessages(topics []string, consumer sarama.Consumer, messageChan chan *sarama.ConsumerMessage, signals chan os.Signal) {
	log.Println("Start consuming messages...")
	for _, topic := range topics {
		partitions, err := consumer.Partitions(topic)
		if err != nil {
			log.Printf("Error getting partitions for topic %s: %v", topic, err)
			continue
		}
		for _, partition := range partitions {
			go func(topic string, partition int32) {
				partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
				if err != nil {
					log.Printf("Error creating partition consumer for topic %s, partition %d: %v", topic, partition, err)
					return
				}
				defer func() {
					if err := partitionConsumer.Close(); err != nil {
						log.Printf("Error closing partition consumer for topic %s, partition %d: %v", topic, partition, err)
					}
				}()

				// Process messages
				for {
					select {
					case msg := <-partitionConsumer.Messages():
						messageChan <- msg
						// log.Printf("Received message: Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
						// 	msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
					case err := <-partitionConsumer.Errors():
						log.Printf("Error consuming message: %v", err)
					case <-signals:
						log.Println("Received shutdown signal. Closing consumer...")
					}
				}
			}(topic, partition)
		}
	}

}
