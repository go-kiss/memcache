package memcache

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestMetaSetGet(t *testing.T) {
	c, _ := New(os.Getenv("MC_ADDRESS"), 2, 100)
	k, v, f, ttl, ttlDelta, wait, ctx, nv := []byte("KIANA"), []byte("KASLANA"), uint32(114514), int64(300), int64(20), int64(2), context.Background(), []byte("KALLEN")

	// Normal set
	sr, err := c.MetaSet(ctx, k, v, MetaSetOptions{
		ReturnCasToken: true,
		SetFlag:        f,
		SetTTL:         ttl,
	})
	if err != nil {
		t.Error(err)
	}
	if sr.CasToken.value == 0 {
		t.Error("CAS Incorrect")
	}

	item, err := c.MetaGet(ctx, k, MetaGetOptions{
		ReturnCasToken: true,
		ReturnFlags:    true,
		ReturnSize:     true,
		ReturnTTL:      true,
		ReturnValue:    true,
		SetTTL:         ttl + ttlDelta,
	})
	if err != nil {
		t.Error(err)
	}
	t.Logf("%+v", item)
	if !bytes.Equal(item.Value, v) {
		t.Error("Value Incorrect")
	}
	if item.CasToken != sr.CasToken {
		t.Error("CAS Incorrect")
	}
	if item.Flags != f {
		t.Error("Flag Incorrect")
	}
	if item.TTL != ttl {
		t.Error("TTL Incorrect")
	}
	if item.Size != uint64(len(v)) {
		t.Error("Size Incorrect")
	}

	// Hit, LastAccess, SetTTL
	time.Sleep(time.Duration(wait) * time.Second)
	item, err = c.MetaGet(ctx, k, MetaGetOptions{
		ReturnHit:        true,
		ReturnLastAccess: true,
		ReturnTTL:        true,
	})
	if err != nil {
		t.Error(err)
	}
	t.Logf("%+v", item)
	if item.Hit != 1 {
		t.Error("Hit Incorrect")
	}
	if item.LastAccess != uint64(wait) {
		t.Error("LastAccess Incorrect")
	}
	if item.TTL != ttl+ttlDelta-wait {
		t.Error("SetTTL Incorrect")
	}

	// append
	_, err = c.MetaSet(ctx, k, nv, MetaSetOptions{Mode: MetaSetModeAppend})
	if err != nil {
		t.Error(err)
	}
	item, err = c.MetaGet(ctx, k, MetaGetOptions{ReturnValue: true, ReturnCasToken: true})
	t.Logf("%+v", item)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(item.Value, append(v, nv...)) {
		t.Error("Append incorrect")
	}

	_, err = c.MetaDelete(ctx, k, MetaDeletOptions{CasToken: item.CasToken})
	if err != nil {
		t.Error(err)
	}
	item, err = c.MetaGet(ctx, k, MetaGetOptions{ReturnValue: true, ReturnCasToken: true})
	if err != ErrCacheMiss {
		t.Error("Delete Fail.")
	}
}

func TestMetaSetCAS(t *testing.T) {
	c, _ := New(os.Getenv("MC_ADDRESS"), 2, 100)
	k, v, ctx, ttl := []byte("KASLANA"), []byte("KIANA"), context.Background(), int64(300)

	gr, err := c.MetaGet(ctx, k, MetaGetOptions{
		ReturnCasToken: true,
		NewWithTTL:     ttl,
		ReturnTTL:      true,
	})
	if err != nil {
		t.Error(err)
	}
	t.Logf("%+v", gr)
	if gr.TTL != ttl {
		t.Error("NewWithTTL Error")
	}

	// Normal set
	_, err = c.MetaSet(ctx, k, v, MetaSetOptions{
		CasToken: casToken{0, true},
	})
	if err != ErrCASConflict {
		t.Error("CAS Invalid")
	}

	// Cas Set
	_, err = c.MetaSet(ctx, k, v, MetaSetOptions{
		CasToken: gr.CasToken,
	})
	if err != nil {
		t.Error("Cas Error", err)
	}

	item, err := c.MetaGet(ctx, k, MetaGetOptions{
		ReturnValue: true,
	})
	if err != nil {
		t.Error(err)
	}
	t.Logf("%q", item.Value)
	if !bytes.Equal(item.Value, v) {
		t.Error("Value Incorrect")
	}
}

func TestAdvancedMeta(t *testing.T) {
	c, _ := New(os.Getenv("MC_ADDRESS"), 2, 100)
	ctx := context.Background()
	key := []byte("NEPTUNE_SEKAI_ICHIBAN_KAWAII")
	value := []byte("https://www.bilibili.com/video/BV1zU4y1w7XE")

	r, err := c.MetaGet(ctx, key, MetaGetOptions{
		NewWithTTL:     10,
		ReturnCasToken: true,
	})
	if err != nil {
		t.Error(err)
	}

	t.Logf("First get: %+v", r)
	if !r.IsWon || r.IsSentWon {
		t.Error("Won fail")
	}

	r2, err := c.MetaGet(ctx, key, MetaGetOptions{ReturnSize: true})
	if err != nil {
		t.Error(err)
	}
	t.Logf("Second get: %+v", r2)
	if r2.IsWon || !r2.IsSentWon {
		t.Error("Sent Won fail")
	}

	r, err = c.MetaSet(ctx, key,
		value,
		MetaSetOptions{
			CasToken: r.CasToken,
		})
	if err != nil {
		t.Error(err)
	}
	t.Logf("Set response: %+v", r)

	item, err := c.MetaGet(context.Background(), key,
		MetaGetOptions{
			ReturnValue: true,
		})
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(item.Value, value) {
		t.Error("Set/Get failed")
	}
	t.Logf("Final get: %+v", item)
}

func TestMetaArithmetic(t *testing.T) {
	c, _ := New(os.Getenv("MC_ADDRESS"), 2, 100)
	ctx, k, iv, d, ttl := context.Background(), []byte("KALLEN"), uint64(20), uint64(11), int64(20)
	item, err := c.MetaArithmetic(ctx, k, MetaArithmeticOptions{
		InitialValue:   iv,
		NewWithTTL:     ttl,
		ReturnValue:    true,
		ReturnCasToken: true,
	})
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(item.Value, []byte(strconv.FormatUint(iv, 10))) {
		t.Errorf("Initial value error. got %q should be %q", item.Value, iv)
	}
	item, err = c.MetaArithmetic(ctx, k, MetaArithmeticOptions{
		Delta:       d,
		CasToken:    item.CasToken,
		ReturnValue: true,
	})
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(item.Value, []byte(strconv.FormatUint(iv+d, 10))) {
		t.Errorf("Increase value error. got %q should be %q", item.Value, iv+d)
	}
	item, err = c.MetaArithmetic(ctx, k, MetaArithmeticOptions{
		Delta:       d,
		CasToken:    item.CasToken,
		Mode:        MetaArithmeticModeDecrement,
		ReturnValue: true,
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
	k, err := os.ReadFile("binaryKey.png")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = c.MetaSet(ctx, k, []byte{}, MetaSetOptions{BinaryKey: true})
	if err != nil {
		t.Error(err)
	}
	_, err = c.MetaGet(ctx, k, MetaGetOptions{BinaryKey: true})
	if err != nil {
		t.Error(err)
	}
	_, err = c.MetaGet(ctx, k, MetaGetOptions{ReturnValue: true, BinaryKey: true})
	if err != nil {
		t.Error("Binary Key Error.", err)
	}
}
