package events

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/John-Santa/go-cqrs/models"
	"github.com/nats-io/nats.go"
)

type NatsEventStore struct {
	conn            *nats.Conn
	feedCreatedSub  *nats.Subscription
	feedCreatedChan chan CreatedFeedMessage
}

func NewNats(url string) (*NatsEventStore, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}

	return &NatsEventStore{conn: conn}, nil
}

func (nes *NatsEventStore) Close() {
	if nes.conn != nil {
		nes.conn.Close()
	}
	if nes.feedCreatedSub != nil {
		nes.feedCreatedSub.Unsubscribe()
	}
	close(nes.feedCreatedChan)
}

func (nes *NatsEventStore) encodeMessage(msg Message) ([]byte, error) {
	b := bytes.Buffer{}
	err := gob.NewEncoder(&b).Encode(msg)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (nes *NatsEventStore) PublishCreatedFeed(ctx context.Context, feed *models.Feed) error {
	msg := CreatedFeedMessage{
		ID:          feed.ID,
		Title:       feed.Title,
		Description: feed.Description,
		CreatedAt:   feed.CreatedAt,
	}
	data, err := nes.encodeMessage(msg)
	if err != nil {
		return err
	}
	return nes.conn.Publish(msg.Type(), data)
}

func (nes *NatsEventStore) decodeMessage(data []byte, msg interface{}) error {
	b := bytes.Buffer{}
	b.Write(data)
	return gob.NewDecoder(&b).Decode(msg)
}

func (nes *NatsEventStore) OnCreatedFeed(f func(CreatedFeedMessage)) (err error) {
	msg := CreatedFeedMessage{}
	nes.feedCreatedSub, err = nes.conn.Subscribe(msg.Type(), func(m *nats.Msg) {
		nes.decodeMessage(m.Data, &msg)
		f(msg)
	})
	return
}

func (nes *NatsEventStore) SubscribeCreatedFeed(ctx context.Context) (<-chan CreatedFeedMessage, error) {
	msg := CreatedFeedMessage{}
	nes.feedCreatedChan = make(chan CreatedFeedMessage, 64)
	channel := make(chan *nats.Msg, 64)
	var err error
	nes.feedCreatedSub, err = nes.conn.ChanSubscribe(msg.Type(), channel)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			select {
			case m := <-channel:
				nes.decodeMessage(m.Data, &msg)
				nes.feedCreatedChan <- msg
			}
		}
	}()
	return (<-chan CreatedFeedMessage)(nes.feedCreatedChan), nil
}
