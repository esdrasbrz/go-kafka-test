package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers                = []string{"localhost:9092"}
	topic      goka.Stream = "loripsum-stream"
	wordsTopic goka.Stream = "loripsum-words"
	group      goka.Group  = "split-group"
)

func main() {
	cb := func(ctx goka.Context, msg interface{}) {
		body := fmt.Sprintf("%v", msg)
		words := strings.Fields(body)
		for _, word := range words {
			word = strings.ToLower(word)
			ctx.Emit(wordsTopic, word, word)
		}
		log.Printf("emitted %d words", len(words))
	}

	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), cb),
		goka.Output(wordsTopic, new(codec.String)),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			log.Fatalf("error running processor: %v", err)
		} else {
			log.Printf("Processor shutdown cleanly")
		}
	}()

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
	<-wait   // wait for SIGINT/SIGTERM
	cancel() // gracefully stop processor
	<-done
}
