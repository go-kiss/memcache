package memcache

import (
	"context"
	"encoding/base64"
)

type MetaResult struct {
	CasToken   casToken
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
func (c *Client) MetaGet(ctx context.Context, key []byte, opt MetaGetOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaGet(stringfyKey(key, opt.BinaryKey), marshalMGOptions(opt))
		return err
	})
	return
}

// Set set one key
func (c *Client) MetaSet(ctx context.Context, key []byte, value []byte, opt MetaSetOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaSet(stringfyKey(key, opt.BinaryKey), value, marshalMSOptions(opt))
		return err
	})
	return
}

// Delete one key
func (c *Client) MetaDelete(ctx context.Context, key []byte, opt MetaDeletOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaDelete(stringfyKey(key, opt.BinaryKey), marshalMDOptions(opt))
		return err
	})
	return
}

// Apply Arithmetic operation to one key
func (c *Client) MetaArithmetic(ctx context.Context, key []byte, opt MetaArithmeticOptions) (i MetaResult, err error) {
	err = c.do(ctx, func(c *Conn) error {
		i, err = c.MetaArithmetic(stringfyKey(key, opt.BinaryKey), marshalMAOptions(opt))
		return err
	})
	return
}

func stringfyKey(k []byte, useBase64 bool) string {
	if !useBase64 {
		return string(k)
	}
	return base64.StdEncoding.EncodeToString(k)
}
