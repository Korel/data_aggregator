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

	receiverConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/", config.AmqReceive.Credentials.Username, config.AmqReceive.Credentials.Password, config.AmqReceive.Broker.Address, config.AmqReceive.Broker.Port))
	if err != nil {
		log.Fatalf("error connecting to receiver: %s", err)
	}

	forwardConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/", config.AmqForward.Credentials.Username, config.AmqForward.Credentials.Password, config.AmqForward.Broker.Address, config.AmqForward.Broker.Port))
	if err != nil {
		log.Fatalf("error connecting to forwarder: %s", err)
	}

	for _, pubSub := range config.PubSub {
		for _, pb := range pubSub {
			rchan, err := receiverConn.Channel()
			if err != nil {
				log.Fatalf("error getting receiver channel: %s", err)
			}
			fchan, err := forwardConn.Channel()
			if err != nil {
				log.Fatalf("error getting forwarder channel: %s", err)
			}

			if err := rchan.ExchangeDeclare(pb.Receive.Exchange, pb.Receive.ExchangeType, false, false, false, false, nil); err != nil {
				log.Fatalf("error declaring receiver exchange: %s", err)
			}
			if err = fchan.ExchangeDeclare(pb.Forward.Exchange, pb.Forward.ExchangeType, false, false, false, false, nil); err != nil {
				log.Fatalf("error declaring forwarder exchange: %s", err)
			}

			rqueue, err := rchan.QueueDeclare(pb.Receive.Queue, true, false, false, false, nil)
			if err != nil {
				log.Fatalf("error declaring receiver queue: %s", err)
			}

			fqueue, err := fchan.QueueDeclare(pb.Forward.Queue, true, false, false, false, nil)
			if err != nil {
				log.Fatalf("error declaring forwarder queue: %s", err)
			}

			if err = rchan.QueueBind(rqueue.Name, "", pb.Receive.Exchange, false, nil); err != nil {
				log.Fatalf("error binding receiver queue: %s", err)
			}

			if err = fchan.QueueBind(fqueue.Name, "", pb.Forward.Exchange, false, nil); err != nil {
				log.Fatalf("error binding forwarder queue: %s", err)
			}

			deliveries, err := rchan.Consume(rqueue.Name, "", false, false, false, false, nil)
			if err != nil {
				log.Fatalf("error consuming receiver queue: %s", err)
			}

			go func() {
				for d := range deliveries {
					if err := d.Ack(false); err != nil {
						log.Fatalf("error acknowledging delivery: %s", err)
					}
					fchan.Publish(
						pb.Forward.Exchange,
						"",
						false,
						false,
						amqp.Publishing{
							ContentType: "text/plain",
							Body:        d.Body,
						},
					)
				}
			}()

		}
	}

	select {}

}
