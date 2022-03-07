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

// The meta get command is the generic command for retrieving key data from
// memcached. Based on the flags supplied, it can replace all of the commands:
// "get", "gets", "gat", "gats", "touch", as well as adding new options.
func (c *Client) MetaGet(ctx context.Context, opt MetaGetOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.metaCmd("mg", stringfyKey(opt.Key, opt.BinaryKey), opt.marshal(), nil)
		return err
	})
	return
}

// The meta set command a generic command for storing data to memcached. Based
// on the flags supplied, it can replace all storage commands (see token M) as
// well as adds new options.
func (c *Client) MetaSet(ctx context.Context, opt MetaSetOptions) (i MetaResult, err error) {
	if opt.Value == nil {
		opt.Value = []byte{}
	}
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.metaCmd("ms", stringfyKey(opt.Key, opt.BinaryKey), opt.marshal(), opt.Value)
		return err
	})
	return
}

// The meta delete command allows for explicit deletion of items, as well as
// marking items as "stale" to allow serving items as stale during revalidation.
func (c *Client) MetaDelete(ctx context.Context, opt MetaDeletOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.metaCmd("md", stringfyKey(opt.Key, opt.BinaryKey), opt.marshal(), nil)
		return err
	})
	return
}

// The meta arithmetic command allows for basic operations against numerical
// values. This replaces the "incr" and "decr" commands. Values are unsigned
// 64bit integers. Decrementing will reach 0 rather than underflow. Incrementing
// can overflow.
func (c *Client) MetaArithmetic(ctx context.Context, opt MetaArithmeticOptions) (v uint64, i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		if i, err = c.metaCmd("ma", stringfyKey(opt.Key, opt.BinaryKey), opt.marshal(), nil); err != nil {
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
