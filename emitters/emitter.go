package main

import (
	"fmt"
	"log"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "example-stream"
)

func main() {
	emitter, err := goka.NewEmitter(brokers, topic, new(codec.String))
	if err != nil {
		log.Fatalf("Error creating emitter: %v", err)
	}
	defer emitter.Finish()
	for i := 1; ; i++ {
		msg := fmt.Sprintf("some value %d", i)
		_, err = emitter.Emit(msg, msg)
		if err != nil {
			log.Fatalf("error emitting message: %v", err)
		}
	}
}
