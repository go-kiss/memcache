package memcache

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestMetaSetGet(t *testing.T) {
	c, _ := New(os.Getenv("MC_ADDRESS"), 2, 100)

	ctx := context.Background()
	k, v := "KIANA", []byte("KASLANA")
	f := uint32(114514)
	nv := []byte("KALLEN")

	// Normal set
	sr, err := c.MetaSet(ctx, MetaSetOptions{
		Key:         k,
		Value:       v,
		GetCasToken: true,
		SetFlag:     f,
		SetTTL:      300,
	})
	if err != nil {
		t.Error(err)
	}
	if sr.CasToken.value == 0 {
		t.Error("CAS Incorrect")
	}

	item, err := c.MetaGet(ctx, MetaGetOptions{
		Key:         k,
		GetCasToken: true,
		GetFlags:    true,
		GetSize:     true,
		GetTTL:      true,
		GetValue:    true,
		SetTTL:      320,
	})
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(item.Value, v) {
		t.Error("Value Incorrect")
	}
	if item.CasToken != sr.CasToken {
		t.Error("CAS Incorrect")
	}
	if item.Flags != f {
		t.Error("Flag Incorrect")
	}
	if item.TTL != 300 {
		t.Error("TTL Incorrect")
	}
	if item.Size != len(v) {
		t.Error("Size Incorrect")
	}

	// Hit, LastAccess, SetTTL
	time.Sleep(2 * time.Second)
	item, err = c.MetaGet(ctx, MetaGetOptions{
		Key:           k,
		GetHit:        true,
		GetLastAccess: true,
		GetTTL:        true,
	})
	if err != nil {
		t.Error(err)
	}
	if !item.Hit {
		t.Error("Hit Incorrect")
	}
	if item.LastAccess != 2 {
		t.Error("LastAccess Incorrect")
	}
	if item.TTL != 300+20-2 {
		t.Error("SetTTL Incorrect")
	}

	// append
	_, err = c.MetaSet(ctx, MetaSetOptions{Key: k, Value: nv, Mode: MetaSetModeAppend})
	if err != nil {
		t.Error(err)
	}
	item, err = c.MetaGet(ctx, MetaGetOptions{Key: k, GetValue: true, GetCasToken: true})
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(item.Value, append(v, nv...)) {
		t.Error("Append incorrect")
	}

	_, err = c.MetaDelete(ctx, MetaDeletOptions{Key: k, CasToken: item.CasToken})
	if err != nil {
		t.Error(err)
	}
	item, err = c.MetaGet(ctx, MetaGetOptions{Key: k, GetValue: true, GetCasToken: true})
	if err != ErrCacheMiss {
		t.Error("Delete Fail.")
	}
}

func TestMetaSetCAS(t *testing.T) {
	c, _ := New(os.Getenv("MC_ADDRESS"), 2, 100)
	k, v, ctx := "KASLANA", []byte("KIANA"), context.Background()

	gr, err := c.MetaGet(ctx, MetaGetOptions{
		Key:              k,
		GetCasToken:      true,
		SetVivifyWithTTL: 300,
		GetTTL:           true,
	})
	if err != nil {
		t.Error(err)
	}
	if gr.TTL != 300 {
		t.Error("NewWithTTL Error")
	}

	// Normal set
	_, err = c.MetaSet(ctx, MetaSetOptions{
		Key:      k,
		Value:    v,
		CasToken: casToken{0, true},
	})
	if err != ErrCASConflict {
		t.Error("CAS Invalid")
	}

	// Cas Set
	_, err = c.MetaSet(ctx, MetaSetOptions{
		Key:      k,
		Value:    v,
		CasToken: gr.CasToken,
	})
	if err != nil {
		t.Error("Cas Error", err)
	}

	item, err := c.MetaGet(ctx, MetaGetOptions{
		Key:      k,
		GetValue: true,
	})
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(item.Value, v) {
		t.Error("Value Incorrect")
	}
}

func TestAdvancedMeta(t *testing.T) {
	c, _ := New(os.Getenv("MC_ADDRESS"), 2, 100)
	ctx := context.Background()
	key := "NEPTUNE_SEKAI_ICHIBAN_KAWAII"
	value := []byte("https://www.bilibili.com/video/BV1zU4y1w7XE")

	r, err := c.MetaGet(ctx, MetaGetOptions{
		Key:              key,
		SetVivifyWithTTL: 10,
		GetCasToken:      true,
	})
	if err != nil {
		t.Error(err)
	}

	if !r.Won {
		t.Error("Won fail")
	}

	r2, err := c.MetaGet(ctx, MetaGetOptions{Key: key, GetSize: true})
	if err != nil {
		t.Error(err)
	}
	if r2.Won {
		t.Error("Sent Won fail")
	}

	r, err = c.MetaSet(ctx,
		MetaSetOptions{
			Key:      key,
			Value:    value,
			CasToken: r.CasToken,
		})
	if err != nil {
		t.Error(err)
	}

	item, err := c.MetaGet(ctx,
		MetaGetOptions{
			Key:      key,
			GetValue: true,
		})
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(item.Value, value) {
		t.Error("Set/Get failed")
	}
}

func TestMetaArithmetic(t *testing.T) {
	c, _ := New(os.Getenv("MC_ADDRESS"), 2, 100)

	ctx := context.Background()
	k := "KALLEN"
	var iv, d, ttl uint64 = math.Float64bits(math.Pow(2, 9)), 11, 20

	v, item, err := c.MetaArithmetic(ctx, MetaArithmeticOptions{
		Key:              k,
		InitialValue:     iv,
		SetVivifyWithTTL: ttl,
		GetValue:         true,
		GetCasToken:      true,
	})
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(item.Value, []byte(strconv.FormatUint(iv, 10))) {
		t.Errorf("Initial value error. got %q should be %q", item.Value, iv)
	}
	if v != iv {
		t.Errorf("Return Value error.")
	}
	v, item, err = c.MetaArithmetic(ctx, MetaArithmeticOptions{
		Key:      k,
		Delta:    d,
		CasToken: item.CasToken,
		GetValue: true,
	})
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(item.Value, []byte(strconv.FormatUint(iv+d, 10))) {
		t.Errorf("Increase value error. got %q should be %q", item.Value, iv+d)
	}
	if v != iv+d {
		t.Errorf("Return Value error.")
	}
	_, item, err = c.MetaArithmetic(ctx, MetaArithmeticOptions{
		Key:      k,
		Delta:    d,
		CasToken: item.CasToken,
		Mode:     MetaArithmeticModeDecrement,
		GetValue: true,
	})
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(item.Value, []byte(strconv.FormatUint(iv, 10))) {
		t.Errorf("Decrease value error. got %q should be %q", item.Value, iv)
	}
}

func TestBinaryKey(t *testing.T) {
	c, _ := New(os.Getenv("MC_ADDRESS"), 2, 100)
	ctx := context.Background()

	id := uint32(65432)
	k := make([]byte, 4)
	binary.BigEndian.PutUint32(k[:], id)

	_, err := c.MetaSet(ctx, MetaSetOptions{BinaryKey: k})
	if err != nil {
		t.Error(err)
	}
	_, err = c.MetaGet(ctx, MetaGetOptions{BinaryKey: k})
	if err != nil {
		t.Error(err)
	}
	_, err = c.MetaGet(ctx, MetaGetOptions{BinaryKey: k, GetValue: true})
	if err != nil {
		t.Error("Binary Key Error.", err)
	}
}
