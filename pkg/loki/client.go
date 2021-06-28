package loki

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"loki-client-test/pkg/logproto"
	"net/http"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"

	"github.com/go-kit/kit/log"
)

const (
	protoContentType = "application/x-protobuf"
	JSONContentType  = "application/json"
)

type entry struct {
	tenantID string
	labels   model.LabelSet
	logproto.Entry
}

type Client struct {
	logger         log.Logger
	cfg            Config
	client         *http.Client
	quit           chan struct{}
	entries        chan entry
	externalLabels model.LabelSet

	// once sync.Once
	// wg   sync.WaitGroup
}

type Config struct {
	URL       string
	BatchWait time.Duration //todo: move
	BatchSize int

	ExternalLabels model.LabelSet `yaml:"external_labels,omitempty"` //todo: validate
	Timeout        time.Duration  `yaml:"timeout"`
	TenantID       string         `yaml:"tenant_id"`
}

func New(cfg Config) (*Client, error) {
	if cfg.URL == "" {
		return nil, errors.New("url not specifed")
	}
	logger := level.NewFilter(log.NewLogfmtLogger(os.Stdout), level.AllowDebug())
	c := &Client{
		logger:         logger,
		cfg:            cfg,
		client:         newHttpClient(cfg.Timeout),
		quit:           make(chan struct{}),
		entries:        make(chan entry),
		externalLabels: cfg.ExternalLabels,
	}
	return c, nil
}

func newHttpClient(timeout time.Duration) *http.Client {
	t := &http.Transport{
		IdleConnTimeout:     timeout,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     100,
		DisableCompression:  true,
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: t,
	}
}

func (c *Client) Run() {
	batches := map[string]*batch{}

	maxWaitCheckFrequency := c.cfg.BatchWait / 10
	maxWaitCheck := time.NewTicker(maxWaitCheckFrequency)

	for {
		select {
		case <-c.quit:
			return
		case e := <-c.entries:
			batch, ok := batches[e.tenantID]

			// If the batch doesn't exist yet, we create a new one with the entry
			if !ok {
				batches[e.tenantID] = newBatch(e)
				break
			}
			// If adding the entry to the batch will increase the size over the max
			// size allowed, we do send the current batch and then create a new one
			if batch.sizeBytesAfter(e) > c.cfg.BatchSize {
				c.sendBatch(e.tenantID, batch)

				batches[e.tenantID] = newBatch(e)
				break
			}
			batch.add(e)

		case <-maxWaitCheck.C:
			for tenantID, batch := range batches {
				if batch.age() < c.cfg.BatchWait {
					continue
				}
				c.sendBatch(tenantID, batch)
				delete(batches, tenantID)
			}
		}
	}
}

// todo exp retry
func (c *Client) sendBatch(tenantID string, batch *batch) (int, error) {
	level.Info(c.logger).Log("msg", "sending entries...", nil)
	buf, entriesCount, err := batch.encode()
	if err != nil {
		level.Error(c.logger).Log("msg", "error encoding batch", "err", err)
	}
	level.Info(c.logger).Log("msg", "encoded entries", "entriesCount", entriesCount)

	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.Timeout)
	defer cancel()

	return c.sendRequest(ctx, buf)
}

func (c *Client) sendRequest(ctx context.Context, buf []byte) (int, error) {
	req, err := http.NewRequest("POST", c.cfg.URL, bytes.NewReader(buf))
	if err != nil {
		return -1, err
	}
	// req = req.WithContext(ctx)
	req.Header.Set("Content-Type", protoContentType)
	req.Header.Set("User-Agent", "loki-go-client")
	if c.cfg.TenantID != "" {
		req.Header.Set("X-Scope-OrgID", c.cfg.TenantID)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		level.Error(c.logger).Log("msg", "error sending batch", "err", err)
		return -1, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(req.Body, 1024))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		level.Error(c.logger).Log("msg", "error sending batch", resp.Status, resp.StatusCode, line)
		return resp.StatusCode, fmt.Errorf("server errror %s %d %s", resp.Status, resp.StatusCode, line)
	}
	return resp.StatusCode, err
}

func (c *Client) Send(ls model.LabelSet, t time.Time, s string) error {
	if len(c.externalLabels) > 0 {
		ls = c.externalLabels.Merge(ls)
	}
	tenantID := c.cfg.TenantID

	c.entries <- entry{tenantID, ls, logproto.Entry{
		Timestamp: t,
		Line:      s,
	}}
	return nil
}
