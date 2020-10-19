package main

import (
	"github.com/esdrasbrz/go-kafka-test/processors"
	"github.com/lovoo/goka"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "example-stream"
	group   goka.Group  = "example-group"
)

func main() {
	processors.RunProcessor(brokers, topic, group)
}
