package memcache

import (
	"bytes"
	"context"
	"testing"

	"sniper/util/conf"
)

func TestClientGet(t *testing.T) {
	addr := conf.GetString("MC_DEFAULT_HOSTS")
	c, _ := New(addr, 2, 100)

	if c.pool.Len() != 2 {
		t.Error("invalid init conns")
	}

	c.Set(context.Background(), &Item{Key: "foo", Value: []byte("bar")})
	item, err := c.Get(context.Background(), "foo")
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(item.Value, []byte("bar")) {
		t.Error("Set/Get failed")
	}
}

func BenchmarkClientGet(b *testing.B) {
	c, _ := New("127.0.0.1:11211", 1, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Set(context.Background(), &Item{Key: "foo", Value: []byte("bar")}); err != nil {
			b.Error(err)
		}
		if _, err := c.Get(context.Background(), "foo"); err != nil {
			b.Error(err)
		}
	}
}
