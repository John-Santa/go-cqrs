package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/John-Santa/go-cqrs/models"
	elastic "github.com/elastic/go-elasticsearch/v7"
)

type ElasticSearchRepository struct {
	client *elastic.Client
}

func NewElastic(url string) (*ElasticSearchRepository, error) {
	client, err := elastic.NewClient(elastic.Config{
		Addresses: []string{url},
	})

	if err != nil {
		return nil, err
	}
	return &ElasticSearchRepository{client}, nil
}

func (esr *ElasticSearchRepository) Close() {
	// TODO
}

func (esr *ElasticSearchRepository) IndexFeed(ctx context.Context, feed models.Feed) error {
	body, _ := json.Marshal(feed)
	_, err := esr.client.Index(
		"feeds",
		bytes.NewReader(body),
		esr.client.Index.WithDocumentID(feed.ID),
		esr.client.Index.WithContext(ctx),
		esr.client.Index.WithRefresh("wait_for"),
	)
	return err
}

func (esr *ElasticSearchRepository) SearchFeed(ctx context.Context, query string) (results []models.Feed, err error) {
	var buf bytes.Buffer

	searchQuery := map[string]interface{}{
		"query": map[string]interface{}{
			"multi_match": map[string]interface{}{
				"query": query,
				"fields": []string{
					"title",
					"description",
				},
				"fuzziness":        3,
				"cutoff_frequency": 0.0001,
			},
		},
	}
	if err = json.NewEncoder(&buf).Encode(searchQuery); err != nil {
		return nil, err
	}

	res, err := esr.client.Search(
		esr.client.Search.WithContext(ctx),
		esr.client.Search.WithIndex("feeds"),
		esr.client.Search.WithBody(&buf),
		esr.client.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := res.Body.Close(); err != nil {
			results = nil
		}
	}()

	if res.IsError() {
		return nil, errors.New(res.String())
	}

	var eRes map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&eRes); err != nil {
		return nil, err
	}
	var feeds []models.Feed
	// Hit es cada elemento que hace match con la query
	for _, hit := range eRes["hits"].(map[string]interface{})["hits"].([]interface{}) {
		var feed models.Feed
		source := hit.(map[string]interface{})["_source"]
		marshal, err := json.Marshal(source)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(marshal, &feed); err == nil {
			feeds = append(feeds, feed)
		}
	}
	return feeds, nil
}
