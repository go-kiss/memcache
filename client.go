package memcache

import (
	"context"
	"net"
	"time"

	"github.com/bilibili/net/pool"
)

// Client memcache client
type Client struct {
	addr string
	pool pool.Pooler
}

// New init client
func New(addr string, initialCap int, maxCap int) (*Client, error) {
	opts := pool.Options{
		Dialer: func(ctx context.Context) (pool.Closer, error) {
			var d net.Dialer
			nc, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}

			c := NewConn(nc)

			return &pooledConn{nc: nc, c: c}, nil
		},
		PoolSize:     maxCap,
		MinIdleConns: initialCap,
		IdleTimeout:  time.Minute,
	}

	return &Client{pool: pool.New(opts)}, nil
}

type pooledConn struct {
	nc net.Conn
	c  *Conn
}

func (pc *pooledConn) Close() error {
	return pc.nc.Close()
}

// PoolStats 返回连接池状态
func (c *Client) PoolStats() *pool.Stats {
	return c.pool.Stats()
}

func (c *Client) do(ctx context.Context, fn func(c *Conn) error) error {
	mc, err := c.pool.Get(ctx)
	if err != nil {
		return err
	}

	pc := mc.C.(*pooledConn)

	if d, ok := ctx.Deadline(); ok {
		pc.nc.SetDeadline(d)
	} else {
		pc.nc.SetDeadline(time.Time{})
	}

	err = fn(pc.c)
	defer c.put(mc, err)

	return err
}

func (c *Client) put(pc *pool.Conn, err error) {
	if IsResumableErr(err) {
		c.pool.Put(pc)
		return
	}

	// TODO 复用 bufio 对象
	c.pool.Remove(pc)
}

// Add only set new key
func (c *Client) Add(ctx context.Context, item *Item) error {
	return c.do(ctx, func(c *Conn) error {
		return c.Add(item)
	})
}

// CompareAndSwap cas set
func (c *Client) CompareAndSwap(ctx context.Context, item *Item) error {
	return c.do(ctx, func(c *Conn) error {
		return c.CompareAndSwap(item)
	})
}

// Decrement decr key
func (c *Client) Decrement(ctx context.Context, key string, delta uint64) (d uint64, err error) {
	err = c.do(ctx, func(c *Conn) error {
		d, err = c.Decrement(key, delta)
		return err
	})

	return
}

// Delete delete key
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.do(ctx, func(c *Conn) error {
		return c.Delete(key)
	})
}

// Get get one key
func (c *Client) Get(ctx context.Context, key string) (i *Item, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.Get(key)
		return err
	})

	return
}

// GetMulti get multi keys
func (c *Client) GetMulti(ctx context.Context, keys []string) (is map[string]*Item, err error) {
	err = c.do(ctx, func(c *Conn) error {
		is, err = c.GetMulti(keys)
		return err
	})

	return
}

// Increment incr key
func (c *Client) Increment(ctx context.Context, key string, delta uint64) (d uint64, err error) {
	err = c.do(ctx, func(c *Conn) error {
		d, err = c.Increment(key, delta)
		return err
	})

	return
}

// Replace set old key
func (c *Client) Replace(ctx context.Context, item *Item) error {
	return c.do(ctx, func(c *Conn) error {
		return c.Replace(item)
	})
}

// Set set key
func (c *Client) Set(ctx context.Context, item *Item) error {
	return c.do(ctx, func(c *Conn) error {
		return c.Set(item)
	})
}

// Touch change ttl
func (c *Client) Touch(ctx context.Context, key string, seconds int32) error {
	return c.do(ctx, func(c *Conn) error {
		return c.Touch(key, seconds)
	})
}

// Close close all connection
func (c *Client) Close() {
	c.pool.Close()
}
