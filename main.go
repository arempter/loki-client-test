package main

import (
	"fmt"
	"loki-client-test/pkg/loki"
	"time"

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

	c.Send(nil, time.Now(), "test line 5")
	c.Send(nil, time.Now(), "test line 6")
	c.Send(nil, time.Now(), "test line 7")
	c.Send(nil, time.Now(), "test line 8")
	c.Send(nil, time.Now(), "test line 9")
	c.Send(nil, time.Now(), "test line 10")
	c.Send(nil, time.Now(), "test line 11")

	select {}
}
