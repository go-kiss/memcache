package memcache

import (
	"context"
	"net"
	"time"

	"git.bilibili.co/go/net/pool"
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

func (c *Client) get(ctx context.Context) (*pooledConn, *pool.Conn) {
	mc, _ := c.pool.Get(ctx)
	pc := mc.C.(*pooledConn)

	if d, ok := ctx.Deadline(); ok {
		pc.nc.SetDeadline(d)
	} else {
		pc.nc.SetDeadline(time.Time{})
	}

	return pc, mc
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
	pc, pc2 := c.get(ctx)

	err := pc.c.Add(item)
	c.put(pc2, err)

	return err
}

// CompareAndSwap cas set
func (c *Client) CompareAndSwap(ctx context.Context, item *Item) error {
	pc, pc2 := c.get(ctx)

	err := pc.c.CompareAndSwap(item)
	c.put(pc2, err)

	return err
}

// Decrement decr key
func (c *Client) Decrement(ctx context.Context, key string, delta uint64) (uint64, error) {
	pc, pc2 := c.get(ctx)

	v, err := pc.c.Decrement(key, delta)
	c.put(pc2, err)

	return v, err
}

// Delete delete key
func (c *Client) Delete(ctx context.Context, key string) error {
	pc, pc2 := c.get(ctx)

	err := pc.c.Delete(key)
	c.put(pc2, err)

	return err
}

// Get get one key
func (c *Client) Get(ctx context.Context, key string) (*Item, error) {
	pc, pc2 := c.get(ctx)

	item, err := pc.c.Get(key)
	c.put(pc2, err)

	return item, err
}

// GetMulti get multi keys
func (c *Client) GetMulti(ctx context.Context, keys []string) (map[string]*Item, error) {
	pc, pc2 := c.get(ctx)

	items, err := pc.c.GetMulti(keys)
	c.put(pc2, err)

	return items, err
}

// Increment incr key
func (c *Client) Increment(ctx context.Context, key string, delta uint64) (uint64, error) {
	pc, pc2 := c.get(ctx)

	v, err := pc.c.Increment(key, delta)
	c.put(pc2, err)

	return v, err
}

// Replace set old key
func (c *Client) Replace(ctx context.Context, item *Item) error {
	pc, pc2 := c.get(ctx)

	err := pc.c.Replace(item)
	c.put(pc2, err)

	return err
}

// Set set key
func (c *Client) Set(ctx context.Context, item *Item) error {
	pc, pc2 := c.get(ctx)

	err := pc.c.Set(item)
	c.put(pc2, err)

	return err
}

// Touch change ttl
func (c *Client) Touch(ctx context.Context, key string, seconds int32) error {
	pc, pc2 := c.get(ctx)

	err := pc.c.Touch(key, seconds)
	c.put(pc2, err)

	return err
}

// Close close all connection
func (c *Client) Close() {
	c.pool.Close()
}
