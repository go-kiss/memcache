package memcache

import (
	"context"
)

type MetaResult struct {
	CasToken   int64
	Flags      uint32
	Hit        uint32
	Key        string
	LastAccess uint64
	Opaque     string
	Size       uint64
	TTL        int64
	Value      []byte
	IsWon      bool
	IsStale    bool
	IsSentWon  bool
}

// Get get one key
func (c *Client) MetaGet(ctx context.Context, key string, opt MetaGetOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaGet(key, marshalMGOptions(opt))
		return err
	})
	return
}

// Set set one key
func (c *Client) MetaSet(ctx context.Context, key string, value []byte, opt MetaSetOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaSet(key, value, marshalMSOptions(opt))
		return err
	})
	return
}

// Delete one key
func (c *Client) MetaDelete(ctx context.Context, key string, opt MetaDeletOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaDelete(key, marshalMDOptions(opt))
		return err
	})
	return
}

// Apply Arithmetic operation to one key
func (c *Client) MetaArithmetic(ctx context.Context, key string, opt MetaArithmeticOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaArithmetic(key, marshalMAOptions(opt))
		return err
	})
	return
}
