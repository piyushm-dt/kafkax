package producer

import (
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
	brokers  []string
	topic    string
}

type ProducerMessage struct {
	Name string
	IP   string
}

func NewProducerConfig() sarama.Config {
	config := *sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	return config
}

func NewProducer(brokers []string, topic string) (Producer, error) {
	config := NewProducerConfig()
	p, err := sarama.NewSyncProducer(brokers, &config)
	if err != nil {
		log.Println("Unable to setup producer", "err", err)
		return Producer{}, err
	}
	return Producer{producer: p, brokers: brokers, topic: topic}, nil

}

func (p *Producer) Send(name, ip string) {
	value := ProducerMessage{
		Name: name,
		IP:   ip,
	}
	msg := &sarama.ProducerMessage{
		Topic:     p.topic,
		Key:       sarama.StringEncoder("key1"),
		Value:     sarama.ByteEncoder(encode(value)),
		Timestamp: time.Now(),
	}
	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		log.Println("Error while producing message to Kafka", "err", err)
	}
	log.Println("successfully produced message to Kafka", "partition", partition, "offset", offset)
}

func encode(msg ProducerMessage) []byte {
	b, _ := json.Marshal(msg)
	return b
}
