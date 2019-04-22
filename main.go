package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"io"
	"log"
	"strings"
)

type DealSearchResult struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore float64 `json:"max_score"`
		Hits     []struct {
			Index string  `json:"_index"`
			Type  string  `json:"_type"`
			ID    string  `json:"_id"`
			Score float64 `json:"_score"`
			Deal  Deal    `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type Deal struct {
	id    string
	Title string `json:"title"`
	Value int    `json:"value"`
}

func (d Deal) ToJSON() io.Reader {
	buf := bytes.Buffer{}
	encoder := json.NewEncoder(&buf)
	encoder.Encode(d)
	return &buf
}

type UpdateDeal struct {
	Doc Deal `json:"doc"`
}

func (d UpdateDeal) ToJSON() io.Reader {
	buf := bytes.Buffer{}
	encoder := json.NewEncoder(&buf)
	encoder.Encode(d)
	return &buf
}

const dealsIndexKey = "deals"

func main() {
	es, _ := elasticsearch.NewDefaultClient()

	//createDeal(es, Deal{"1","Microsoft buys amazon", 20})
	//createDeal(es, Deal{"2","Amazon buys apple", 200})
	//createDeal(es, Deal{"3","CJ buys Microsoft", 5})

	//updateDeal(es, Deal{"1", "Microsoft xxxx", 250})

	fmt.Println(freeTextSearch(es, "apple"))
	fmt.Println(dealsGreaterThan(es, 220))
}

func dealsGreaterThan(es *elasticsearch.Client, amount int) []Deal {
	const valueQuery = `{
  "query": {
    "bool": {
      "must": { "match_all": {} },
      "filter": {
        "range": {
          "value": {
            "gte": %d
          }
        }
      }
    }
  }
}`
	query := fmt.Sprintf(valueQuery, amount)

	res, err := esapi.SearchRequest{
		Index: []string{dealsIndexKey},
		Body:  strings.NewReader(query),
	}.Do(context.Background(), es)

	if err != nil {
		log.Fatal(err)
	}

	defer res.Body.Close()
	var searchResult DealSearchResult
	json.NewDecoder(res.Body).Decode(&searchResult)

	var deals []Deal
	for _, hit := range searchResult.Hits.Hits {
		deals = append(deals, hit.Deal)
	}

	return deals
}

func freeTextSearch(es *elasticsearch.Client, query string) []Deal {
	res, _ := esapi.SearchRequest{
		Index: []string{dealsIndexKey},
		Query: fmt.Sprintf("q=%s", query),
	}.Do(context.Background(), es)

	defer res.Body.Close()
	var searchResult DealSearchResult
	json.NewDecoder(res.Body).Decode(&searchResult)

	var deals []Deal
	for _, hit := range searchResult.Hits.Hits {
		deals = append(deals, hit.Deal)
	}

	return deals
}

func updateDeal(es *elasticsearch.Client, update Deal) {
	esapi.UpdateRequest{
		Index:      dealsIndexKey,
		DocumentID: update.id,
		Body:       UpdateDeal{update}.ToJSON(),
	}.Do(context.Background(), es)
}

func printDeal(es *elasticsearch.Client, id string) {
	res, _ := esapi.GetRequest{
		Index:      dealsIndexKey,
		DocumentID: "1",
	}.Do(context.Background(), es)

	defer res.Body.Close()

	var dealBody map[string]interface{}

	json.NewDecoder(res.Body).Decode(&dealBody)

	log.Println(dealBody)
}

func createDeal(client *elasticsearch.Client, deal Deal) {
	req := esapi.IndexRequest{
		Index:      dealsIndexKey,
		DocumentID: deal.id,
		Body:       deal.ToJSON(),
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), client)

	if err != nil {
		log.Fatal("failed to insert deal", err)
	}

	defer res.Body.Close()
}
