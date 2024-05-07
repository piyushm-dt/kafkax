package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/IBM/sarama"
)

type ConsumerGroupHandler struct {
	ready chan bool
}

func (handler *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(handler.ready)
	log.Println("session setup")
	return nil
}

func (handler *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("session cleanup")
	return nil
}

func (handler *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			HandleMessage(message)
			session.MarkMessage(message, "")
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

type ConsumerGroup struct {
	Config  sarama.Config
	Handler sarama.ConsumerGroup
}

func NewConsumerConfig() sarama.Config {
	config := *sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Consumer.Return.Errors = true
	return config
}

func NewCustomConsumer(brokers, topics []string, group string) ConsumerGroup {
	c := ConsumerGroup{}
	c.Config = NewConsumerConfig()
	return c
}

func (cg ConsumerGroup) Close() {
	if err := cg.Handler.Close(); err != nil {
		log.Println("unable to close consumer group")
	}
}

func (cg ConsumerGroup) Consume(brokers, topics []string, group string) error {
	consumergroup, err := sarama.NewConsumerGroup(brokers, group, &cg.Config)
	if err != nil {
		log.Println("unable to setup consumer group", "err", err)
		return err
	}
	handler := ConsumerGroupHandler{
		ready: make(chan bool),
	}

	go func() {
		for err := range consumergroup.Errors() {
			log.Println("error received on consumergroup", "err", err)
		}
	}()

	//below will start the actual consumption loop
	go func() {
		for {
			log.Println("consuming")
			ctx := context.Background()
			if err = consumergroup.Consume(ctx, topics, &handler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					log.Println("Error consumer group closed", "err", err)
					return
				}
				log.Println("unable to consume from consumer group", "err", err)
				panic(err)
			}
			if ctx.Err() != nil {
				log.Println("Error context canceled", "err", err)
				return
			}
			handler.ready = make(chan bool)
		}
	}()

	<-handler.ready
	log.Println("consumer is up and running")

	return nil
}

type Message struct {
	Name string `Name:"name"`
	IP   string `IP:"ip"`
}

func HandleMessage(msg *sarama.ConsumerMessage) {
	log.Println(msg.Headers, msg.Timestamp)
	log.Println("Topic", msg.Topic, "Partition", msg.Partition, "Offset", msg.Offset)
	log.Println("Key", string(msg.Key), "Value", string(msg.Value))

	var sms Message
	json.Unmarshal(msg.Value, &sms)
	log.Println(sms)
}
