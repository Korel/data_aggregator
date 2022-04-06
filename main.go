package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/go-redis/redis/v8"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RedisConnection struct {
	RedisClient  *redis.Client
	RedisContext context.Context
}

func failOnError(err error, str string) {
	if err != nil {
		log.Fatalf("%s: %s", str, err)
	}
}

func setupRedis(config *Config) RedisConnection {
	redisConnection := RedisConnection{
		RedisClient: redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", config.Redis.Address, config.Redis.Port),
			Password: config.Redis.Credentials.Password,
			DB:       0, // use default DB
		}),
		RedisContext: context.Background(),
	}
	_, err := redisConnection.RedisClient.Ping(redisConnection.RedisContext).Result()
	failOnError(err, "Failed to connect to redis server")
	log.Printf("Connected to redis server -> %s", fmt.Sprintf("%s:%d", config.Redis.Address, config.Redis.Port))
	return redisConnection
}

func setupRabbitmq(config *Config, redisConnection *RedisConnection, consumeCallback func(*PubSubConfig, <-chan amqp.Delivery, *amqp.Channel, *RedisConnection)) {
	sourceConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/",
		config.AmqSource.Credentials.Username,
		config.AmqSource.Credentials.Password,
		config.AmqSource.Broker.Address,
		config.AmqSource.Broker.Port))
	failOnError(err, fmt.Sprintf("Failed to connect to source RabbitMQ address -> %s:%d", config.AmqSource.Broker.Address, config.AmqSource.Broker.Port))
	log.Printf("Connected to source RabbitMQ address -> %s:%d", config.AmqSource.Broker.Address, config.AmqSource.Broker.Port)
	targetConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/",
		config.AmqTarget.Credentials.Username,
		config.AmqTarget.Credentials.Password,
		config.AmqTarget.Broker.Address,
		config.AmqTarget.Broker.Port))
	failOnError(err, fmt.Sprintf("Failed to connect to target RabbitMQ address -> %s:%d", config.AmqTarget.Broker.Address, config.AmqTarget.Broker.Port))
	log.Printf("Connected to target RabbitMQ address -> %s:%d", config.AmqTarget.Broker.Address, config.AmqTarget.Broker.Port)
	// Create pub_sub channels, declare exchanges and queues
	for _, pubSub := range config.PubSub {
		for _, pb := range pubSub {
			sourceChannel, err := sourceConn.Channel()
			failOnError(err, "Failed to open the source channel")
			targetChannel, err := targetConn.Channel()
			failOnError(err, "Failed to open the target channel")

			err = sourceChannel.ExchangeDeclare(
				pb.Source.Exchange,     // exchange name
				pb.Source.ExchangeType, // exchange type
				false,                  // durable
				false,                  // auto-deleted
				false,                  // internal
				false,                  // noWait
				nil,                    // arguments
			)
			failOnError(err, "Failed to declare the source exchange")
			log.Printf("Declared source exchange: %s", pb.Source.Exchange)

			err = targetChannel.ExchangeDeclare(
				pb.Target.Exchange,     // exchange name
				pb.Target.ExchangeType, // exchange type
				false,                  // durable
				false,                  // auto-deleted
				false,                  // internal
				false,                  // noWait
				nil,                    // arguments
			)
			failOnError(err, "Failed to declare the target exchange")
			log.Printf("Declared target exchange: %s", pb.Target.Exchange)

			sourceQueue, err := sourceChannel.QueueDeclare(
				pb.Source.Queue, // queue name
				true,            // durable
				false,           // delete when unused
				false,           // exclusive
				false,           // no-wait
				nil,             // arguments
			)
			failOnError(err, "Failed to declare the source queue")
			log.Printf("Declared source queue: %s", pb.Source.Queue)

			targetQueue, err := targetChannel.QueueDeclare(
				pb.Target.Queue, // queue name
				true,            // durable
				false,           // delete when unused
				false,           // exclusive
				false,           // no-wait
				nil,             // arguments
			)
			failOnError(err, "Failed to declare the target queue")
			log.Printf("Declared target queue: %s", pb.Target.Queue)

			err = sourceChannel.QueueBind(
				sourceQueue.Name,   // queue name
				"",                 // routing key
				pb.Source.Exchange, // exchange
				false,              // noWait
				nil,                // arguments
			)
			failOnError(err, "Failed to bind the source queue")
			log.Printf("Bound to source queue: %s", pb.Source.Queue)

			err = targetChannel.QueueBind(
				targetQueue.Name,   // queue name
				"",                 // routing key
				pb.Target.Exchange, // exchange
				false,              // noWait
				nil,                // arguments
			)
			failOnError(err, "Failed to bind the target queue")
			log.Printf("Bound to target queue: %s", pb.Target.Queue)

			deliveries, err := sourceChannel.Consume(
				sourceQueue.Name, // queue name
				"",               // consumer
				false,            // auto-ack
				false,            // exclusive
				false,            // no-local
				false,            // no-wait
				nil,              // arguments
			)
			failOnError(err, "Failed to start consuming the source queue")
			log.Printf("Starting to consume source queue: %s", pb.Source.Queue)

			go consumeCallback(&pb, deliveries, targetChannel, redisConnection)
		}
	}
}

func rabbitmqConsumeCallback(pb *PubSubConfig, deliveries <-chan amqp.Delivery, targetChannel *amqp.Channel, redisConnection *RedisConnection) {
	for d := range deliveries {
		if err := d.Ack(false); err != nil {
			log.Fatalf("Error acknowledging delivery: %s", err)
		}
		// TODO: Filtering will be implemented here
		// Filter(d.Body)

		targetChannel.Publish(
			pb.Target.Exchange, // exchange
			"",                 // routing key
			false,              // mandatory
			false,              // immediate
			amqp.Publishing{ // message
				ContentType: "text/plain",
				Body:        d.Body,
			},
		)
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <config.yaml>\n", os.Args[0])
		os.Exit(1)
	}
	config, err := ParseConfig(os.Args[1])
	failOnError(err, "Failed to parse config file")

	redisConnection := setupRedis(config)
	failOnError(err, "Failed to get redis key")
	setupRabbitmq(config, &redisConnection, rabbitmqConsumeCallback)

	// The consuming and forwarding are done in goroutines.
	select {} // block foreve
}
