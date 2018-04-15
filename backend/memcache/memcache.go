package memcache

import (
	"time"
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/ltick/crypt/backend"
)

type Client struct{
	client    *memcache.Client
	logger backend.Logger
}

func New(machines []string) (*Client, error) {
	return &Client{
		client: memcache.New(machines...),
		logger: nil,
	}, nil
}

func (c *Client) Get(key string) ([]byte, error) {
	kv, err := c.client.Get(key)
	if err != nil {
		return nil, err
	}
	if kv == nil {
		return nil, fmt.Errorf("memcache: key was not found error: %s not found.", key)
	}
	return kv.Value, err
}

func (c *Client) List(key string) (backend.KVPairs, error) {
	list := make(backend.KVPairs, 0)
	return list, nil
}

func (c *Client) Set(key string, value []byte) error {
	item := &memcache.Item {
		Key: key,
		Value: value,
		Flags: 0,
		Expiration: 0,
	}
	err := c.client.Set(item)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Delete(key string) error {
	err := c.client.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Watch(key string, stop chan bool) <-chan *backend.Response {
	respChan := make(chan *backend.Response, 0)
	go func() {
		for {
			b, err := c.Get(key)
			if err != nil {
				respChan <- &backend.Response{nil, err}
				time.Sleep(time.Second * 5)
				continue
			}
			respChan <- &backend.Response{b, nil}
		}
	}()
	return respChan
}

func (c *Client) SetLogger(l backend.Logger) {
	c.logger = l
}