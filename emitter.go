package main

import (
	"fmt"
	"log"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "example-stream"
	group   goka.Group  = "example-group"
)

func main() {
	emitter, err := goka.NewEmitter(brokers, topic, new(codec.String))
	if err != nil {
		log.Fatalf("Error creating emitter: %v", err)
	}
	defer emitter.Finish()
	for i := 1; ; i++ {
		time.Sleep(1 * time.Second)
		msg := fmt.Sprintf("some value %d", i)
		err = emitter.EmitSync("some-key", msg)
		if err != nil {
			log.Fatalf("error emitting message: %v", err)
		}
		log.Printf("emitted %s", msg)
	}
}
