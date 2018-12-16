package memcache

import (
	"context"
	"net"
	"time"
)

// Client memcache client
type Client struct {
	addr    string
	pool    Pool
	timeout time.Duration
}

// New init client
func New(addr string, initialCap int, maxCap int) (*Client, error) {
	Close := func(v interface{}) error { return v.(*pooledConn).nc.Close() }

	factory := func(ctx context.Context) (interface{}, error) {
		var d net.Dialer
		nc, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}

		c := NewConn(nc)

		return &pooledConn{nc: nc, c: c}, nil
	}

	poolConfig := &PoolConfig{
		InitialCap:  initialCap,
		MaxCap:      maxCap,
		Factory:     factory,
		Close:       Close,
		IdleTimeout: 15 * time.Second,
	}

	p, err := NewChannelPool(poolConfig)
	if err != nil {
		return nil, err
	}

	return &Client{pool: p, timeout: 1 * time.Second}, nil
}

type pooledConn struct {
	nc net.Conn
	c  *Conn
}

// SetTimeout 设置超时时间
func (c *Client) SetTimeout(t time.Duration) {
	if t > 0 {
		c.timeout = t
	}
}

func (c *Client) get(ctx context.Context) *pooledConn {
	mc, _ := c.pool.Get(ctx)
	pc := mc.(*pooledConn)

	d := time.Now().Add(c.timeout)
	if d2, ok := ctx.Deadline(); ok && d2.Before(d) {
		d = d2
	}

	pc.nc.SetDeadline(d)

	return pc
}

func (c *Client) put(pc *pooledConn, err error) {
	if IsResumableErr(err) {
		c.pool.Put(pc)
		return
	}

	// TODO 复用 bufio 对象
	pc.nc.Close()
}

// Add only set new key
func (c *Client) Add(ctx context.Context, item *Item) error {
	pc := c.get(ctx)

	err := pc.c.Add(item)
	c.put(pc, err)

	return err
}

// CompareAndSwap cas set
func (c *Client) CompareAndSwap(ctx context.Context, item *Item) error {
	pc := c.get(ctx)

	err := pc.c.CompareAndSwap(item)
	c.put(pc, err)

	return err
}

// Decrement decr key
func (c *Client) Decrement(ctx context.Context, key string, delta uint64) (uint64, error) {
	pc := c.get(ctx)

	v, err := pc.c.Decrement(key, delta)
	c.put(pc, err)

	return v, err
}

// Delete delete key
func (c *Client) Delete(ctx context.Context, key string) error {
	pc := c.get(ctx)

	err := pc.c.Delete(key)
	c.put(pc, err)

	return err
}

// Get get one key
func (c *Client) Get(ctx context.Context, key string) (*Item, error) {
	pc := c.get(ctx)

	item, err := pc.c.Get(key)
	c.put(pc, err)

	return item, err
}

// GetMulti get multi keys
func (c *Client) GetMulti(ctx context.Context, keys []string) (map[string]*Item, error) {
	pc := c.get(ctx)

	items, err := pc.c.GetMulti(keys)
	c.put(pc, err)

	return items, err
}

// Increment incr key
func (c *Client) Increment(ctx context.Context, key string, delta uint64) (uint64, error) {
	pc := c.get(ctx)

	v, err := pc.c.Increment(key, delta)
	c.put(pc, err)

	return v, err
}

// Replace set old key
func (c *Client) Replace(ctx context.Context, item *Item) error {
	pc := c.get(ctx)

	err := pc.c.Replace(item)
	c.put(pc, err)

	return err
}

// Set set key
func (c *Client) Set(ctx context.Context, item *Item) error {
	pc := c.get(ctx)

	err := pc.c.Set(item)
	c.put(pc, err)

	return err
}

// Touch change ttl
func (c *Client) Touch(ctx context.Context, key string, seconds int32) error {
	pc := c.get(ctx)

	err := pc.c.Touch(key, seconds)
	c.put(pc, err)

	return err
}

// Close close all connection
func (c *Client) Close() {
	c.pool.Release()
}
