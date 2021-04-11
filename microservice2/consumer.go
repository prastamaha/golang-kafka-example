package main

import (
	"encoding/json"
	"flag"
	"log"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gopkg.in/mgo.v2"
)

// mongo variable

type MongoStore struct {
	session *mgo.Session
}

var (
	mongoAddr  string
	mongoCol   string
	mongoDB    string
	mongoUser  string
	mongoPass  string
	mongoStore = MongoStore{}
)

// kafka variable
var (
	kafkaAddr     string
	kafkaTopic    string
	kafkaConGroup string
)

// ops object struct
type Ops struct {
	Name     string `json:"name"`
	Division string `json:"division"`
	Position string `json:"position"`
}

func main() {

	// mongo flag
	flag.StringVar(&mongoAddr, "mongo-addr", "localhost:27017", "[Address]:[Port] Mongodb listen")
	flag.StringVar(&mongoDB, "mongo-db", "job", "Mongodb target database")
	flag.StringVar(&mongoCol, "mongo-col", "ops", "Mongodb target collection")
	flag.StringVar(&mongoUser, "mongo-username", "", "Mongodb username")
	flag.StringVar(&mongoPass, "mongo-password", "", "Mongodb password")

	// kafka flag
	flag.StringVar(&kafkaAddr, "kafka-address", "localhost:9092", "[Address]:[Port] kafka running")
	flag.StringVar(&kafkaTopic, "kafka-topic", "kafka-riset-topic", "Topics to be subcribe by consumer")
	flag.StringVar(&kafkaConGroup, "kafka-group", "kafka-riset-group", "Consumer group name")

	flag.Parse()

	// Create MongoDB Connection
	s := initialiseMongo()
	mongoStore.session = s

	// Listen to kafka topic
	consumeFromKafka()

}

// connect to MongoDB
func initialiseMongo() *mgo.Session {

	info := &mgo.DialInfo{
		Addrs:    []string{mongoAddr},
		Timeout:  60 * time.Second,
		Database: mongoDB,
		Username: mongoUser,
		Password: mongoPass,
	}

	s, err := mgo.DialWithInfo(info)
	if err != nil {
		panic(err)
	}

	return s

}

func consumeFromKafka() {

	// more configuration, check on https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaAddr,
		"group.id":          kafkaConGroup,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

	log.Printf("Start Subscribe to kafka topic %s on %s\n", kafkaTopic, kafkaAddr)

	consumer.SubscribeTopics([]string{kafkaTopic}, nil)

	// listen kafka with infinite loop
	for {
		msg, err := consumer.ReadMessage(-1)

		if err == nil {
			log.Printf("Received event from kafka topic %s: %s\n", msg.TopicPartition, string(msg.Value))
			opsString := string(msg.Value)
			saveToMongo(opsString)
		} else {
			log.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}

	consumer.Close()
}

func saveToMongo(opsString string) {

	// Get MongoDB Collection
	mcol := mongoStore.session.DB(mongoDB).C(mongoCol)

	// Unmarshal ops into struct
	var ops Ops
	b := []byte(opsString)
	err := json.Unmarshal(b, &ops)
	if err != nil {
		panic(err)
	}

	// insert data into MongoDB
	err = mcol.Insert(ops)
	if err != nil {
		panic(err)
	}

	log.Printf("Successfully saved data into MongoDB: %s\n", opsString)

}
