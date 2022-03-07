package memcache

import (
	"context"
	"encoding/base64"
	"strconv"
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
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaGet(stringfyKey(opt.Key, opt.BinaryKey), opt.marshal())
		return err
	})
	return
}

// Set set one key
func (c *Client) MetaSet(ctx context.Context, opt MetaSetOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaSet(stringfyKey(opt.Key, opt.BinaryKey), opt.Value, opt.marshal())
		return err
	})
	return
}

// Delete one key
func (c *Client) MetaDelete(ctx context.Context, opt MetaDeletOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaDelete(stringfyKey(opt.Key, opt.BinaryKey), opt.marshal())
		return err
	})
	return
}

// Apply Arithmetic operation to one key
func (c *Client) MetaArithmetic(ctx context.Context, opt MetaArithmeticOptions) (v uint64, i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		if i, err = c.MetaArithmetic(stringfyKey(opt.Key, opt.BinaryKey), opt.marshal()); err != nil {
			return err
		}
		if opt.GetValue {
			v, err = strconv.ParseUint(string(i.Value), 10, 64)
			return err
		}
		return nil
	})
	return
}

func stringfyKey(key string, binaryKey []byte) string {
	if len(binaryKey) > 0 {
		return base64.StdEncoding.EncodeToString(binaryKey)
	}
	return key
}
