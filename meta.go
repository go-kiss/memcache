package memcache

import (
	"context"
	"encoding/base64"
)

type MetaResult struct {
	CasToken   casToken
	Flags      uint32
	Key        string
	LastAccess uint64
	Opaque     string
	Size       int
	TTL        int64
	Value      []byte
	Hit        bool
	Won        bool
	Stale      bool

	isNoOp bool
}

// Get get one key
func (c *Client) MetaGet(ctx context.Context, key string, opt MetaGetOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaGet(stringfyKey(key, opt.BinaryKey), opt.marshal())
		return err
	})
	return
}

// Set set one key
func (c *Client) MetaSet(ctx context.Context, key string, value []byte, opt MetaSetOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaSet(stringfyKey(key, opt.BinaryKey), value, opt.marshal())
		return err
	})
	return
}

// Delete one key
func (c *Client) MetaDelete(ctx context.Context, key string, opt MetaDeletOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaDelete(stringfyKey(key, opt.BinaryKey), opt.marshal())
		return err
	})
	return
}

// Apply Arithmetic operation to one key
func (c *Client) MetaArithmetic(ctx context.Context, key string, opt MetaArithmeticOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaArithmetic(stringfyKey(key, opt.BinaryKey), opt.marshal())
		return err
	})
	return
}

func stringfyKey(key string, binaryKey []byte) string {
	if len(binaryKey) > 0 {
		return base64.StdEncoding.EncodeToString(binaryKey)
	}
	return key
}
