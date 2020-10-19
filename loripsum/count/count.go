package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers                = []string{"localhost:9092"}
	wordsTopic goka.Stream = "loripsum-words"
	group      goka.Group  = "count-group"
)

func main() {
	cb := func(ctx goka.Context, msg interface{}) {
		var counter int64 = 0
		if val := ctx.Value(); val != nil {
			counter = val.(int64)
		}
		counter++
		ctx.SetValue(counter)
	}

	g := goka.DefineGroup(group,
		goka.Input(wordsTopic, new(codec.String), cb),
		goka.Persist(new(codec.Int64)),
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
