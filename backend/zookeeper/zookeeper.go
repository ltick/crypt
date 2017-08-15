package zookeeper

import (
	"strings"
	"time"

	"errors"

	"github.com/ltick/crypt/backend"
	"github.com/samuel/go-zookeeper/zk"
)

var connectTimeout time.Duration = 3*time.Second

type Client struct {
	client   *zk.Conn
	user     string
	password string
	errors   chan error
}

func New(machines []string, user string, password string) (*Client, error) {
	for index, machine := range machines {
		machines[index] = strings.TrimSpace(machine)
	}
	client, _, err := zk.Connect(machines, connectTimeout)
	if err != nil {
		return nil, err
	}
	c := &Client{
		client:   client,
		user:     user,
		password: password,
		errors:   make(chan error, 1),
	}
	if err = c.addAuth(); err != nil {
		return nil, err
	}
	go func() {
		for {
			select {
			case err := <-c.errors:
				if err == zk.ErrSessionExpired {
					c.addAuth()
				} else {
					//log
				}
			}
		}
	}()
	return c, nil
}

func (c *Client) Get(key string) ([]byte, error) {
	value, _, err := c.client.Get(key)
	if err != nil {
		c.errors <- err
		return nil, errors.New("zookeeper: Get " + key + " error")
	}
	return value, nil
}

func (c *Client) List(key string) (backend.KVPairs, error) {
	list := make(backend.KVPairs, 0)
	return list, nil
}

func (c *Client) Set(key string, value []byte) error {
	value, stat, err := c.client.Get(key)
	if err != nil {
		if err == zk.ErrNoNode {
			_, err = c.client.Create(key, value, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				return err
			}
		} else {
			c.errors <- err
			return errors.New("zookeeper: Set " + key + " error")
		}
	}
	_, err = c.client.Set(key, value, stat.Version)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Watch(key string, stop chan bool) <-chan *backend.Response {
	respChan := make(chan *backend.Response, 0)
	go func() {
		exists, _, event, err := c.client.ExistsW(key)
		if !exists {
			return
		}
		if err != nil {
			respChan <- &backend.Response{nil, err}
		}
		for {
			select {
			case e := <-event:
				if e.Err != nil {
					respChan <- &backend.Response{nil, e.Err}
				}
				switch e.Type {
				case zk.EventNodeDataChanged:
					value, _, err := c.client.Get(key)
					if err != nil {
						respChan <- &backend.Response{nil, err}
					}
					respChan <- &backend.Response{value, nil}
				}
				return
			}
		}
	}()
	return respChan
}

func (c *Client) addAuth() error {
	if c.user != "" && c.password != "" {
		if err := c.client.AddAuth("digest", []byte(c.user+":"+c.password)); err != nil {
			return err
		}
	}
	return nil
}
