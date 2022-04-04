package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, str string) {
	if err != nil {
		log.Fatalf("%s: %s", str, err)
	}
}

func setupRedis(config *Config) {
	//TODO: find a library and implement
}

func setupRabbitmq(config *Config, consumeCallback func(*PubSubConfig, <-chan amqp.Delivery, *amqp.Channel)) {
	sourceConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/",
		config.AmqSource.Credentials.Username,
		config.AmqSource.Credentials.Password,
		config.AmqSource.Broker.Address,
		config.AmqSource.Broker.Port))
	failOnError(err, "Failed to connect to source RabbitMQ")
	log.Printf("Connected to source RabbitMQ")
	targetConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/",
		config.AmqTarget.Credentials.Username,
		config.AmqTarget.Credentials.Password,
		config.AmqTarget.Broker.Address,
		config.AmqTarget.Broker.Port))
	failOnError(err, "Failed to connect to target RabbitMQ")
	log.Printf("Connected to target RabbitMQ")
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

			go consumeCallback(&pb, deliveries, targetChannel)
		}
	}
}

func rabbitmqConsumeCallback(pb *PubSubConfig, deliveries <-chan amqp.Delivery, targetChannel *amqp.Channel) {
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
	config, err := Parse("config.yaml")
	failOnError(err, "Failed to parse config file")
	log.Printf("RabbitMQ Source Address: %s\n", config.AmqSource.Broker.Address)
	log.Printf("RabbitMQ Target Address: %s\n", config.AmqTarget.Broker.Address)

	setupRedis(config)

	setupRabbitmq(config, rabbitmqConsumeCallback)

	// The consuming and forwarding are done in goroutines.
	select {} // block foreve
}
