package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/John-Santa/go-cqrs/events"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	NatsAddress string `envconfig:"NATS_ADDRESS"`
}

func main() {
	var cfg Config
	err := envconfig.Process("", &cfg)

	if err != nil {
		log.Fatalf("Error processing env vars: %v", err)
	}

	hub := NewHub()

	nats, err := events.NewNats(fmt.Sprintf("nats://%s", cfg.NatsAddress))
	if err != nil {
		log.Fatalf("Error connecting to nats: %v", err)
	}

	err = nats.OnCreatedFeed(func(m events.CreatedFeedMessage) {
		hub.Broadcast(newCreatedFeedMessage(m.ID, m.Title, m.Description, m.CreatedAt), nil)
	})

	if err != nil {
		log.Fatalf("Error subscribing to nats: %v", err)
	}

	events.SetEventStore(nats)

	defer events.Close()

	go hub.Run()

	http.HandleFunc("/ws", hub.HandleWebSocket)
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}

}
