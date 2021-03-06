package mock

import (
	"errors"
	"path"
	"strings"
	"time"

	"github.com/ltick/crypt/backend"
	"fmt"
)

var mockedStore map[string][]byte

type Client struct{
	logger backend.Logger
}

func New(machines []string) (*Client, error) {
	if mockedStore == nil {
		mockedStore = make(map[string][]byte, 2)
	}
	return &Client{}, nil
}

func (c *Client) Get(key string) ([]byte, error) {
	if v, ok := mockedStore[key]; ok {
		return v, nil
	}
	err := fmt.Errorf("mock: key was not found error: %s not found.", key)
	return nil, err
}

func (c *Client) List(key string) (backend.KVPairs, error) {
	var list backend.KVPairs
	dir := path.Clean(key) + "/"
	for k, v := range mockedStore {
		if strings.HasPrefix(k, dir) {
			list = append(list, &backend.KVPair{Key: k, Value: v})
		}
	}
	return list, nil
}

func (c *Client) Set(key string, value []byte) error {
	mockedStore[key] = value
	return nil
}

func (c *Client) Delete(key string) error {
	delete(mockedStore, key)
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