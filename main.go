package main

import (
	"fmt"
	"io"
	"log"
	"loki-client-test/pkg/loki"
	"net/http"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
)

func main() {

	ls := model.LabelSet{"job": "cron"}
	cfg := loki.Config{
		URL:            "http://localhost:3100/loki/api/v1/push",
		TenantID:       "",
		Timeout:        3 * time.Second,
		BatchWait:      3 * time.Second,
		BatchSize:      5,
		ExternalLabels: ls,
	}
	c, err := loki.New(cfg)
	if err != nil {
		fmt.Print("Failed to create client")
	}
	go c.Run()

	// c.Send(nil, time.Now(), "test line 5")
	// c.Send(nil, time.Now(), "test line 6")
	// c.Send(nil, time.Now(), "test line 7")
	// c.Send(nil, time.Now(), "test line 8")
	// c.Send(nil, time.Now(), "test line 9")
	// c.Send(nil, time.Now(), "test line 10")
	// c.Send(nil, time.Now(), "test line 11")

	http.HandleFunc("/loki/api/v1/push", getPromHandler(c))
	log.Fatal(http.ListenAndServe(":3101", nil))
}

//todo: move it to proper place and ref
func getPromHandler(c *loki.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		api := loki.NewPromAPI()
		buf, err := io.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			fmt.Println("failed to read response body", err)
		}
		buf, err = snappy.Decode(nil, buf)
		if err != nil {
			fmt.Println("failed to decode response body", err)
		}
		pr, err := api.Decode(buf)
		if err != nil {
			fmt.Println("failed to decode response body", err)
		}
		if pr != nil {
			for _, s := range pr.Streams {
				fmt.Println("stream", s)
				for _, e := range s.Entries {
					c.Send(nil, time.Now(), e.Line)
				}
			}
		}
	}
}
