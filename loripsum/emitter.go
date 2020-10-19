package main

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "loripsum-stream"
	apiURL  string      = "https://loripsum.net/api/5/plaintext"
)

func getText() string {
	resp, err := http.Get(apiURL)
	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	return string(body)
}

func getTextKey(text string) string {
	algorithm := fnv.New32a()
	algorithm.Write([]byte(text))
	return fmt.Sprint(algorithm.Sum32())
}

func main() {
	emitter, err := goka.NewEmitter(brokers, topic, new(codec.String))
	if err != nil {
		log.Fatalf("Error creating emitter: %v", err)
	}
	defer emitter.Finish()

	for {
		text := getText()
		key := getTextKey(text)
		_, err = emitter.Emit(key, text)
		if err != nil {
			log.Fatalf("error emitting message: %v", err)
		}
	}
}
