package memcache

import (
	"bytes"
	"context"
	"os"
	"testing"
)

func TestClientGet(t *testing.T) {
	c, _ := New(os.Getenv("MC_ADDRESS"), 2, 100)

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
	c, _ := New(os.Getenv("MC_ADDRESS"), 1, 100)

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
