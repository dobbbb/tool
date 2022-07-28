package es

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"github.com/tidwall/gjson"
	"io"
	"log"
	"sync"
	"time"
)

var once sync.Once

type CompareContext struct {
	SrcHost     string
	SrcUserName string
	SrcPassword string
	SrcIndex    string
	Query       string
	srcClient   *elastic.Client

	DstHost     string
	DstUserName string
	DstPassword string
	DstIndex    string
	dstClient   *elastic.Client

	BatchSize int
	Slices    int
	ConcurNum int
	Field     string
	IsRepair  bool

	DiffIds []string
}

func (c *CompareContext) Init() {
	once.Do(func() {
		c.DiffIds = make([]string, 0)

		srcOptions := []elastic.ClientOptionFunc{
			elastic.SetURL(c.SrcHost),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(true),
			elastic.SetBasicAuth(c.SrcUserName, c.SrcPassword),
		}
		client, err := elastic.NewClient(srcOptions...)
		if err != nil {
			log.Fatalf("init client err:%s\n", c.SrcHost)
		}
		c.srcClient = client

		if c.DstHost == "" {
			c.dstClient = client
			if c.SrcIndex == c.DstIndex {
				log.Fatal("need srcIndex != desIndex, when srcAddr == desAddr")
			}
		}

		desOptions := []elastic.ClientOptionFunc{
			elastic.SetURL(c.DstHost),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(true),
			elastic.SetBasicAuth(c.DstUserName, c.DstPassword),
		}
		desClient, err := elastic.NewClient(desOptions...)
		if err != nil {
			log.Fatalf("int desClinet err:%s\n", c.DstHost)
		}
		c.dstClient = desClient
	})
}

func (c *CompareContext) ScrollSliceCompare() {
	wg := sync.WaitGroup{}
	for i := 0; i < c.Slices; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sq := elastic.NewSliceQuery().Id(i).Max(c.Slices)
			c.Compare(sq, nil)
		}(i)
	}
	wg.Wait()
}

func (c *CompareContext) CustomConcurrencyCompare() {
	var min, max int64
	searchResult, err := c.srcClient.Search().Index(c.SrcIndex).Size(1).Sort(c.Field, true).Do(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	if searchResult.TotalHits() > 0 && len(searchResult.Hits.Hits) > 0 {
		min = gjson.GetBytes(searchResult.Hits.Hits[0].Source, c.Field).Int()
	} else {
		log.Fatal("no data was fund")
	}

	searchResult, err = c.srcClient.Search().Index(c.SrcIndex).Size(1).Sort(c.Field, false).Do(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	if searchResult.TotalHits() > 0 && len(searchResult.Hits.Hits) > 0 {
		max = gjson.GetBytes(searchResult.Hits.Hits[0].Source, c.Field).Int()
	} else {
		log.Fatal("no data was fund")
	}

	wg := sync.WaitGroup{}
	step := (max - min) / int64(c.ConcurNum)
	for start, end := min, min+step; start < max; start, end = end, end+step {
		wg.Add(1)
		body := map[string]interface{}{
			"query": map[string]interface{}{
				"range": map[string]interface{}{
					c.Field: map[string]interface{}{
						"gte": start,
						"lte": end,
					},
				},
			},
			"sort": []interface{}{"_doc"},
		}
		go func() {
			defer wg.Done()
			c.Compare(nil, body)
		}()
	}
	wg.Wait()
}

func (c *CompareContext) Compare(sq *elastic.SliceQuery, body interface{}) {
	svc := c.srcClient.Scroll(c.SrcIndex).Size(c.BatchSize).Body(body)
	if svc == nil {
		log.Fatalf("request %s faild", c.SrcHost)
	}
	if sq != nil {
		svc = svc.Slice(sq)
	}

	pages := 0
	docs := 0
	logCount := 0

	begin := time.Now().Unix()
	for {
		res, err := svc.Do(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if res == nil {
			log.Fatal("expected results != nil; got nil")
		}
		if res.Hits == nil {
			log.Fatal("expected results.Hits != nil; got nil")
		}

		pages++

		ids := make([]string, 0)
		id2Hit := make(map[string]*elastic.SearchHit)
		for i, hit := range res.Hits.Hits {
			ids = append(ids, hit.Id)
			id2Hit[hit.Id] = res.Hits.Hits[i]
			docs++
		}
		c.compare(ids, id2Hit)
		if docs/100000 > logCount {
			logCount = docs / 100000
			end := time.Now().Unix()
			log.Printf("pages:%d  docs:%d  cast:%ds", pages, docs, end-begin)
		}

		if len(res.ScrollId) == 0 {
			log.Fatalf("expected scrollId in results; got %q", res.ScrollId)
		}
	}
	end := time.Now().Unix()
	log.Printf("pages:%d  docs:%d  cast:%ds", pages, docs, end-begin)
}

func (c *CompareContext) compare(ids []string, id2Hit map[string]*elastic.SearchHit) {
	searchResult, err := c.dstClient.Search().Index(c.DstIndex).Query(elastic.NewTermsQueryFromStrings("id", ids...)).Size(len(ids)).Do(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	id2Hit1 := make(map[string]*elastic.SearchHit)
	if searchResult.TotalHits() > 0 && len(searchResult.Hits.Hits) > 0 {
		for i, hit := range searchResult.Hits.Hits {
			id2Hit1[hit.Id] = searchResult.Hits.Hits[i]
		}
	}
	for id, hit := range id2Hit {
		if id2Hit1[id] != nil {
			ss, _ := hit.Source.MarshalJSON()
			ds, _ := id2Hit1[id].Source.MarshalJSON()
			if string(ss) != string(ds) {
				c.reCompare(id, string(ss), string(ds))
			}
		} else {
			log.Printf("id:%s was not fund", hit.Id)
			c.DiffIds = append(c.DiffIds, id)
		}
	}
}

/**
 * scroll是快照查询，当对比出数据不一致时，需要通过普通查询再次进行对比
 */
func (c *CompareContext) reCompare(id, ss, ds string) {
	searchResult, err := c.dstClient.Search().Index(c.DstIndex).Query(elastic.NewTermQuery("id", id)).Size(1).Do(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	if searchResult.TotalHits() > 0 && len(searchResult.Hits.Hits) > 0 {
		ss1, _ := searchResult.Hits.Hits[0].Source.MarshalJSON()
		if string(ss1) != ds {
			log.Printf("id:%s is diff, src:%s des:%ss", id, ss1, ds)
			c.DiffIds = append(c.DiffIds, id)
		}
		return
	}
	log.Printf("id:%s is diff, src:%s des:%ss", id, ss, ds)
	c.DiffIds = append(c.DiffIds, id)
}

func (c *CompareContext) Repair() {
	src := elastic.NewReindexSource().Index(c.SrcIndex).Query(elastic.NewTermsQueryFromStrings("id", c.DiffIds...))
	if c.SrcHost != c.DstHost {
		src.RemoteInfo(
			elastic.NewReindexRemoteInfo().Host(c.SrcHost).
				Username(c.SrcUserName).
				Password(c.SrcPassword),
		)
	}
	dst := elastic.NewReindexDestination().Index(c.DstIndex)
	res, err := c.dstClient.Reindex().Source(src).Destination(dst).Do(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	rb, _ := json.Marshal(res)
	log.Println(string(rb))
}
