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
func (c *Client) MetaGet(ctx context.Context, opt MetaGetOptions) (i MetaResult, err error) {
	key, err := stringfyKey(opt.Key, opt.BinaryKey)
	if err != nil {
		return
	}
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaGet(key, opt.marshal())
		return err
	})
	return
}

// Set set one key
func (c *Client) MetaSet(ctx context.Context, opt MetaSetOptions) (i MetaResult, err error) {
	key, err := stringfyKey(opt.Key, opt.BinaryKey)
	if err != nil {
		return
	}
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaSet(key, opt.Value, opt.marshal())
		return err
	})
	return
}

// Delete one key
func (c *Client) MetaDelete(ctx context.Context, opt MetaDeletOptions) (i MetaResult, err error) {
	key, err := stringfyKey(opt.Key, opt.BinaryKey)
	if err != nil {
		return
	}
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaDelete(key, opt.marshal())
		return err
	})
	return
}

// Apply Arithmetic operation to one key
func (c *Client) MetaArithmetic(ctx context.Context, opt MetaArithmeticOptions) (i MetaResult, err error) {
	key, err := stringfyKey(opt.Key, opt.BinaryKey)
	if err != nil {
		return
	}
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaArithmetic(key, opt.marshal())
		return err
	})
	return
}

func stringfyKey(key string, binaryKey []byte) (string, error) {
	if len(binaryKey) > 0 {
		return base64.StdEncoding.EncodeToString(binaryKey), nil
	}
	if len(key) == 0 {
		return "", ErrMalformedKey
	}
	return key, nil
}
