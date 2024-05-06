package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/bas-baskara/kafka-consumer-grpc/configs"
	pb "github.com/bas-baskara/kafka-consumer-grpc/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var env = configs.EnvGetter()
var initialTopics []string

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

	topics, err := consumer.Topics()
	if err != nil {
		log.Println("Error getting topics", err)
	}

	initialTopics = topics

	// Trap SIGINT to trigger a graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// create channel
	messageChan := make(chan *sarama.ConsumerMessage)

	// consume all initial topics available
	go consumeAllTopics(consumer, messageChan)

	// start worker
	workerNumber, err := strconv.Atoi(env.KAFKA_WORKER_POOL_NUMBER)
	if err != nil {
		log.Fatal("Worker number not set:", err)
		return
	}

	for i := 0; i < workerNumber; i++ {
		go worker(messageChan, *client)
	}

	// Periodically check for new topics and consume messages
	tickerInterval := 10

	tickerSecond, err := strconv.Atoi(env.KAFKA_TICKER_SECOND)
	if err != nil {
		log.Println("Error getting ticker interval in seconds", err)
	} else {
		tickerInterval = tickerSecond
	}

	go func(tickerInterval int) {
		ticker := time.NewTicker(time.Duration(tickerInterval) * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			newConsumer, err := sarama.NewConsumer(brokers, config)
			if err != nil {
				log.Println("error when create new consumer connection", err)
			}

			newTopics, err := newConsumer.Topics()
			if err != nil {
				log.Println("Error getting topics in interval:", err)
				newConsumer.Close()
				continue
			}

			for _, newTopic := range newTopics {
				if !containsTopic(initialTopics, newTopic) {
					initialTopics = append(initialTopics, newTopic)
					go consumeTopic(newTopic, consumer, messageChan)
				}
			}

			if err := newConsumer.Close(); err != nil {
				log.Println("Error closing new consumer in interval:", err)
			}

		}
	}(tickerInterval)

	// wait for shutdown signal
	<-signals
	log.Println("Shutdown signal received. Exiting...")

}

func containsTopic(topics []string, topic string) bool {
	for _, t := range topics {
		if t == topic {
			return true
		}
	}

	return false
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

func consumeAllTopics(consumer sarama.Consumer, messageChan chan *sarama.ConsumerMessage) {
	// subscribe to all topics
	topics, err := consumer.Topics()
	if err != nil {
		log.Println("Error getting topics:", err)
		return
	}

	for _, topic := range topics {
		go consumeTopic(topic, consumer, messageChan)
	}
}

func consumeTopic(topic string, consumer sarama.Consumer, messageChan chan<- *sarama.ConsumerMessage) {
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Printf("error getting partitions for topic %s: %v", topic, err)
		return
	}

	for _, partition := range partitions {
		go func(topic string, partition int32) {
			partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.Printf("error creating partition consumer for topic %s, partition %d: %v", topic, partition, err)
				return
			}

			defer func() {
				if err := partitionConsumer.Close(); err != nil {
					log.Printf("Error closing partition consumer for topic %s, partition %d: %v", topic, partition, err)
				}
			}()

			// process message
			for {
				select {
				case msg := <-partitionConsumer.Messages():
					messageChan <- msg
				case err := <-partitionConsumer.Errors():
					log.Printf("Error consuming message: %v", err)
				}
			}

		}(topic, partition)
	}
}
