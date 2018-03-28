package zookeeper

import (
	"strings"
	"time"
	"context"
	"errors"

	"github.com/ltick/crypt/backend"
	"github.com/samuel/go-zookeeper/zk"
)

var connectTimeout time.Duration = 120 * time.Second

type Client struct {
	client   *zk.Conn
	user     string
	password string
	errors   chan error
	logger   backend.Logger
}

var client *Client

func New(machines []string, user string, password string) (*Client, error) {
	if client != nil {
		return client, nil
	}
	for index, machine := range machines {
		machines[index] = strings.TrimSpace(machine)
	}
	zkClient, _, err := zk.Connect(machines, connectTimeout)
	if err != nil {
		return nil, err
	}
	client = &Client{
		client:   zkClient,
		user:     user,
		password: password,
		errors:   make(chan error, 1),
		logger:   nil,
	}
	if err = client.addAuth(); err != nil {
		return nil, err
	}
	go func() {
		for {
			select {
			case err := <-client.errors:
				if err == zk.ErrSessionExpired {
					client.addAuth()
				} else {
					//log
				}
			}
		}
	}()
	return client, nil
}

func (c *Client) Get(key string) ([]byte, error) {
	value, _, err := c.client.Get(key)
	if err != nil {
		c.errors <- err
		return nil, errors.New("zookeeper: Get " + key + " error: " + err.Error())
	}
	return value, nil
}

func (c *Client) List(key string) (backend.KVPairs, error) {
	list := make(backend.KVPairs, 0)
	listKeys, _, err := c.client.Children(key)
	if err != nil {
		c.errors <- err
		return nil, errors.New("zookeeper: List " + key + " error: " + err.Error())
	}
	for _, listKey := range listKeys {
		listValue, err := c.Get(key + "/" + listKey)
		if err != nil {
			c.errors <- err
			return nil, errors.New("zookeeper: List " + key + " error: " + err.Error())
		}
		list = append(list, &backend.KVPair{Key: listKey, Value: listValue})
	}
	return list, nil
}

func (c *Client) Set(key string, value []byte) error {
	_, stat, err := c.client.Get(key)
	if err != nil {
		if err == zk.ErrNoNode {
			_, err = c.client.Create(key, value, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				return err
			}
		} else {
			c.errors <- err
			return errors.New("zookeeper: Set " + key + " error: " + err.Error())
		}
	}
	_, err = c.client.Set(key, value, stat.Version)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Watch(key string, stop chan bool) <-chan *backend.Response {
	return c.WatchWithContext(context.Background(), key, stop)
}

func (c *Client) WatchWithContext(ctx context.Context, key string, stop chan bool) <-chan *backend.Response {
	respChan := make(chan *backend.Response, 0)
	go func() {
		value, _, event, err := c.client.GetW(key)
		if err != nil {
			respChan <- &backend.Response{nil, err}
		} else {
			respChan <- &backend.Response{value, nil}
		}
		_, cancel := context.WithCancel(ctx)
		for {
			select {
			case <-stop:
				c.client.Close()
				cancel()
				break
			case e := <-event:
				if e.Err != nil {
					respChan <- &backend.Response{nil, e.Err}
				}
				switch e.Type {
				case zk.EventNodeDataChanged:
					value, _, event, err = c.client.GetW(key)
					if err != nil {
						respChan <- &backend.Response{nil, err}
					} else {
						respChan <- &backend.Response{value, nil}
					}
				}
			}
		}
	}()
	return respChan
}

func (c *Client) SetLogger(l backend.Logger) {
	c.logger = l
	c.client.SetLogger(l)
}

func (c *Client) addAuth() error {
	if c.user != "" && c.password != "" {
		if err := c.client.AddAuth("digest", []byte(c.user+":"+c.password)); err != nil {
			return err
		}
	}
	return nil
}
