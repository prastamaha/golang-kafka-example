package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// flag variable
var (
	listen     string
	kafkaAddr  string
	kafkaTopic string
)

// ops object struct
type Ops struct {
	Name     string `json:"name"`
	Division string `json:"division"`
	Position string `json:"position"`
}

func main() {

	// Flag arguments
	flag.StringVar(&listen, "listen", "0.0.0.0:9090", "[Address]:[Port] that will be listened to by the webservice")
	flag.StringVar(&kafkaAddr, "kafka-address", "localhost:9092", "[Address]:[Port] kafka listen")
	flag.StringVar(&kafkaTopic, "kafka-topic", "kafka-riset-topic", "topics to be written by producer")
	flag.Parse()

	// Listen web service with  Mux
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/ops", opsHandler).Methods("POST")
	log.Printf("Service running on %s\n", listen)
	log.Fatalln(http.ListenAndServe(listen, router))
}

func opsHandler(w http.ResponseWriter, r *http.Request) {

	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		panic(err)
	}

	var ops Ops
	err = json.Unmarshal(b, &ops)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	produceToKafka(ops)

	opsString, err := json.Marshal(ops)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("content-type", "aplication/json")

	w.Write(opsString)
}

func produceToKafka(ops Ops) {

	// Convert object into bytes
	opsString, err := json.Marshal(ops)
	if err != nil {
		panic(err)
	}

	// more configuration, check on https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaAddr,
	})
	if err != nil {
		panic(err)
	}

	// Produce messages to topic (asynchronously)
	for _, word := range []string{string(opsString)} {
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
		log.Printf("Event produce to Kafka topic %s: %v\n", kafkaTopic, ops)
	}

}
