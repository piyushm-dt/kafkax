package main

import (
	"encoding/json"
	"io"
	"kafkax/consumer"
	"kafkax/producer"
	"log"

	"github.com/gin-gonic/gin"
)

var (
	KAFKA_BROKERS         = []string{"localhost:9092"}
	KAFKA_TOPICS          = []string{"test"}
	KAFKA_CONSUMER_GROUPS = "group"
)

func main() {
	// init

	//router
	r := gin.Default()
	r.GET("/startproducer", ProducerX)
	r.GET("/startconsumer", ConsumerX)
	r.Run("localhost:8000")
}

func ProducerX(c *gin.Context) {

	producer, err := producer.NewProducer(KAFKA_BROKERS, KAFKA_TOPICS[0])
	if err != nil {
		log.Println("cannot start producer")
		c.JSON(400, "unable to produce messages")
		return
	}
	body, _ := io.ReadAll(c.Request.Body)
	var jsonMap struct {
		N int `N:"n"`
	}
	json.Unmarshal(body, &jsonMap)
	for i := 0; i < jsonMap.N; i++ {
		producer.Send("new message", c.RemoteIP())
	}
	c.JSON(200, "produced messages")

}

func ConsumerX(c *gin.Context) {

	consumer := consumer.NewCustomConsumer(KAFKA_BROKERS, KAFKA_TOPICS, KAFKA_CONSUMER_GROUPS)
	if err := consumer.Consume(KAFKA_BROKERS, KAFKA_TOPICS, KAFKA_CONSUMER_GROUPS); err != nil {
		log.Println("Unable to consume from Kafka", "err", err)
	}
	c.JSON(200, "consumer started")
}

// .\bin\windows\kafka-server-start.bat config/server.properties
// .\bin\zkServer.cmd
