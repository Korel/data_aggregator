package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	config, err := Parse("config.yaml")
	if err != nil {
		log.Fatalf("error parsing config: %s", err)
	}

	log.Printf("RabbitMQ Source Address: %s\n", config.AmqSource.Broker.Address)
	log.Printf("RabbitMQ Target Address: %s\n", config.AmqTarget.Broker.Address)

	receiverConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/", config.AmqSource.Credentials.Username, config.AmqSource.Credentials.Password, config.AmqSource.Broker.Address, config.AmqSource.Broker.Port))
	if err != nil {
		log.Fatalf("Error connecting to source RabbitMQ: %s", err)
	}
	log.Printf("Connected to source RabbitMQ")

	forwardConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/", config.AmqTarget.Credentials.Username, config.AmqTarget.Credentials.Password, config.AmqTarget.Broker.Address, config.AmqTarget.Broker.Port))
	if err != nil {
		log.Fatalf("Error connecting to target RabbitMQ: %s", err)
	}
	log.Printf("Connected to target RabbitMQ")

	for _, pubSub := range config.PubSub {
		for _, pb := range pubSub {
			rchan, err := receiverConn.Channel()
			if err != nil {
				log.Fatalf("Error getting source channel: %s", err)
			}
			fchan, err := forwardConn.Channel()
			if err != nil {
				log.Fatalf("Error getting target channel: %s", err)
			}

			if err := rchan.ExchangeDeclare(
				pb.Source.Exchange,     // exchange name
				pb.Source.ExchangeType, // exchange type
				false,                  // durable
				false,                  // auto-deleted
				false,                  // internal
				false,                  // noWait
				nil,                    // arguments
			); err != nil {
				log.Fatalf("Error declaring source exchange: %s", err)
			}
			log.Printf("Declared source exchange: %s", pb.Source.Exchange)

			if err = fchan.ExchangeDeclare(
				pb.Target.Exchange,     // exchange name
				pb.Target.ExchangeType, // exchange type
				false,                  // durable
				false,                  // auto-deleted
				false,                  // internal
				false,                  // noWait
				nil,                    // arguments
			); err != nil {
				log.Fatalf("Error declaring target exchange: %s", err)
			}
			log.Printf("Declared target exchange: %s", pb.Target.Exchange)

			rqueue, err := rchan.QueueDeclare(
				pb.Source.Queue, // queue name
				true,            // durable
				false,           // delete when unused
				false,           // exclusive
				false,           // no-wait
				nil,             // arguments
			)
			if err != nil {
				log.Fatalf("Error declaring source queue: %s", err)
			}
			log.Printf("Declared source queue: %s", pb.Source.Queue)

			fqueue, err := fchan.QueueDeclare(
				pb.Target.Queue, // queue name
				true,            // durable
				false,           // delete when unused
				false,           // exclusive
				false,           // no-wait
				nil,             // arguments
			)
			if err != nil {
				log.Fatalf("Error declaring target queue: %s", err)
			}
			log.Printf("Declared target queue: %s", pb.Target.Queue)

			if err = rchan.QueueBind(
				rqueue.Name,        // queue name
				"",                 // routing key
				pb.Source.Exchange, // exchange
				false,              // noWait
				nil,                // arguments
			); err != nil {
				log.Fatalf("Error binding source queue: %s", err)
			}
			log.Printf("Bound to source queue: %s", pb.Source.Queue)

			if err = fchan.QueueBind(
				fqueue.Name,        // queue name
				"",                 // routing key
				pb.Target.Exchange, // exchange
				false,              // noWait
				nil,                // arguments
			); err != nil {
				log.Fatalf("Error binding target queue: %s", err)
			}
			log.Printf("Bound to target queue: %s", pb.Target.Queue)

			deliveries, err := rchan.Consume(
				rqueue.Name, // queue name
				"",          // consumer
				false,       // auto-ack
				false,       // exclusive
				false,       // no-local
				false,       // no-wait
				nil,         // arguments
			)
			if err != nil {
				log.Fatalf("Error consuming source queue: %s", err)
			}

			log.Printf("Starting to consume source queue: %s", pb.Source.Queue)

			go func(pb *PubSubConfig) {
				for d := range deliveries {
					if err := d.Ack(false); err != nil {
						log.Fatalf("Error acknowledging delivery: %s", err)
					}

					// TODO: Filtering will be implemented here
					// Filter(d.Body)

					fchan.Publish(
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
			}(&pb)

		}
	}

	// The consuming and forwarding are done in goroutines.
	select {} // block foreve
}
