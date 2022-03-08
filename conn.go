// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
)

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")
)

var (
	crlf            = []byte("\r\n")
	space           = []byte(" ")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultOk        = []byte("OK\r\n")
	resultError     = []byte("ERROR\r\n")
	resultTouched   = []byte("TOUCHED\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
	resultServerErrorPrefix = []byte("SERVER_ERROR ")
)

// Conn is a memcache client.
// It is safe for unlocked use by multiple concurrent goroutines.
type Conn struct {
	rw *bufio.ReadWriter
}

// NewConn create a new memcache connection.
func NewConn(c net.Conn) *Conn {
	r := bufio.NewReader(c)
	w := bufio.NewWriter(c)
	rw := bufio.NewReadWriter(r, w)

	return &Conn{rw}
}

// Item is an item to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string

	// Value is the Item's value.
	Value []byte

	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32

	// Compare and swap ID.
	casid uint64
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *Conn) Get(key string) (*Item, error) {
	if _, err := fmt.Fprintf(c.rw, "get %s\r\n", key); err != nil {
		return nil, err
	}
	if err := c.rw.Flush(); err != nil {
		return nil, err
	}

	items, err := parseGetResponse(c.rw.Reader)
	if err != nil {
		return nil, err
	}

	it, ok := items[key]
	if !ok {
		return nil, ErrCacheMiss
	}

	return it, nil
}

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (c *Conn) GetMulti(keys []string) (map[string]*Item, error) {
	if _, err := fmt.Fprintf(c.rw, "gets %s\r\n", strings.Join(keys, " ")); err != nil {
		return nil, err
	}
	if err := c.rw.Flush(); err != nil {
		return nil, err
	}

	return parseGetResponse(c.rw.Reader)
}

func parseGetResponse(r *bufio.Reader) (map[string]*Item, error) {
	items := make(map[string]*Item)

	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return nil, err
		}
		if bytes.Equal(line, resultEnd) {
			return items, err
		}
		it := new(Item)
		size, err := scanGetResponseLine(line, it)
		if err != nil {
			return nil, err
		}
		it.Value, err = ioutil.ReadAll(io.LimitReader(r, int64(size)+2))
		if err != nil {
			return nil, err
		}
		if !bytes.HasSuffix(it.Value, crlf) {
			return nil, fmt.Errorf("memcache: corrupt get result read")
		}
		it.Value = it.Value[:size]
		items[it.Key] = it
	}
}

// scanGetResponseLine populates it and returns the declared size of the item.
// It does not read the bytes of the item.
func scanGetResponseLine(line []byte, it *Item) (size int, err error) {
	pattern := "VALUE %s %d %d %d\r\n"
	dest := []interface{}{&it.Key, &it.Flags, &size, &it.casid}
	if bytes.Count(line, space) == 3 {
		pattern = "VALUE %s %d %d\r\n"
		dest = dest[:3]
	}
	n, err := fmt.Sscanf(string(line), pattern, dest...)
	if err != nil || n != len(dest) {
		return -1, fmt.Errorf("memcache: unexpected line in get response: %q", line)
	}
	return size, nil
}

// Set writes the given item, unconditionally.
func (c *Conn) Set(item *Item) error {
	return c.populateOne(c.rw, "set", item)
}

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (c *Conn) Add(item *Item) error {
	return c.populateOne(c.rw, "add", item)
}

// Replace writes the given item, but only if the server *does*
// already hold data for this key
func (c *Conn) Replace(item *Item) error {
	return c.populateOne(c.rw, "replace", item)
}

// CompareAndSwap writes the given item that was previously returned
// by Get, if the value was neither modified or evicted between the
// Get and the CompareAndSwap calls. The item's Key should not change
// between calls but all other item fields may differ. ErrCASConflict
// is returned if the value was modified in between the
// calls. ErrNotStored is returned if the value was evicted in between
// the calls.
func (c *Conn) CompareAndSwap(item *Item) error {
	return c.populateOne(c.rw, "cas", item)
}

func (c *Conn) populateOne(rw *bufio.ReadWriter, verb string, item *Item) error {
	if !legalKey(item.Key) {
		return ErrMalformedKey
	}

	var err error
	if verb == "cas" {
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d %d\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value), item.casid)
	} else {
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value))
	}
	if err != nil {
		return err
	}
	if _, err = rw.Write(item.Value); err != nil {
		return err
	}
	if _, err := rw.Write(crlf); err != nil {
		return err
	}
	if err := rw.Flush(); err != nil {
		return err
	}

	line, err := rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultStored):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrNotStored
	case bytes.Equal(line, resultExists):
		return ErrCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrCacheMiss
	}

	return fmt.Errorf("memcache: unexpected response line from %q: %q", verb, string(line))
}

func writeReadLine(rw *bufio.ReadWriter, format string, args ...interface{}) ([]byte, error) {
	_, err := fmt.Fprintf(rw, format, args...)
	if err != nil {
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		return nil, err
	}
	line, err := rw.ReadSlice('\n')
	return line, err
}

func writeExpectf(rw *bufio.ReadWriter, expect []byte, format string, args ...interface{}) error {
	line, err := writeReadLine(rw, format, args...)
	if err != nil {
		return err
	}

	switch {
	case bytes.Equal(line, resultOk):
		return nil
	case bytes.Equal(line, expect):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrNotStored
	case bytes.Equal(line, resultExists):
		return ErrCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrCacheMiss
	}

	return fmt.Errorf("memcache: unexpected response line: %q", string(line))
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *Conn) Delete(key string) error {
	return writeExpectf(c.rw, resultDeleted, "delete %s\r\n", key)
}

// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
func (c *Conn) Increment(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("incr", key, delta)
}

// Decrement atomically decrements key by delta. The return value is
// the new value after being decremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On underflow, the new value is capped at zero and does not wrap
// around.
func (c *Conn) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("decr", key, delta)
}

func (c *Conn) incrDecr(verb, key string, delta uint64) (uint64, error) {
	var val uint64
	line, err := writeReadLine(c.rw, "%s %s %d\r\n", verb, key, delta)
	if err != nil {
		return val, err
	}
	switch {
	case bytes.Equal(line, resultNotFound):
		return val, ErrCacheMiss
	case bytes.HasPrefix(line, resultClientErrorPrefix):
		errMsg := line[len(resultClientErrorPrefix) : len(line)-2]
		return val, errors.New("memcache: client error: " + string(errMsg))
	}

	val, err = strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
	return val, err
}

// Touch updates the expiry for the given key. The seconds parameter is either
// a Unix timestamp or, if seconds is less than 1 month, the number of seconds
// into the future at which time the item will expire. ErrCacheMiss is returned if the
// key is not in the cache. The key must be at most 250 bytes in length.
func (c *Conn) Touch(key string, seconds int32) (err error) {
	return writeExpectf(c.rw, resultTouched, "touch %s %d\r\n", key, seconds)
}

// FlushAll clear all item
func (c *Conn) FlushAll() error {
	return writeExpectf(c.rw, resultOk, "flush_all\r\n")
}

func (c *Conn) ping() error {
	return writeExpectf(c.rw, resultError, "ping\r\n")
}

// IsResumableErr returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func IsResumableErr(err error) bool {
	switch err {
	case ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrMalformedKey:
		return true
	case nil:
		return true
	}
	return false
}

func (c *Conn) metaCmd(cmd, key string, flags []metaFlag, data []byte) (mr MetaResult, err error) {
	if !legalKey(key) {
		err = ErrMalformedKey
		return
	}
	withPayload := data != nil
	if withPayload {
		_, err = fmt.Fprintf(c.rw, "%s %s %d %s\r\n", cmd, key, len(data), buildMetaFlags(flags))
	} else {
		_, err = fmt.Fprintf(c.rw, "%s %s %s\r\n", cmd, key, buildMetaFlags(flags))
	}
	if err != nil {
		return
	}
	if withPayload {
		if _, err = c.rw.Write(data); err != nil {
			return
		}
		if _, err = c.rw.Write(crlf); err != nil {
			return
		}
	}
	if err = c.rw.Flush(); err != nil {
		return
	}
	mr, err = parseMetaResponse(c.rw.Reader)
	return
}

func legalKey(key string) bool {
	if l := len(key); l > 250 || l == 0 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] == 0x7f {
			return false
		}
	}
	return true
}

func parseMetaResponse(r *bufio.Reader) (mr MetaResult, err error) {
	statusLineRaw, err := r.ReadSlice('\n')
	if err != nil {
		return
	}

	status := strings.Fields(string(statusLineRaw))
	code, size, withValue, status := status[0], 0, false, status[1:]
	switch code {
	case "MN":
		mr.isNoOp = true
	case "VA":
		size, err = strconv.Atoi(status[0])
		status, withValue = status[1:], true
	case "NS":
		err = ErrNotStored
	case "EX":
		err = ErrCASConflict
	case "EN", "NF":
		err = ErrCacheMiss
	case "HD":
	default:
		err = fmt.Errorf("memcache: unexpected line in response: %q", statusLineRaw)
	}
	if mr.isNoOp || err != nil {
		return
	}
	if mr, err = obtainMetaFlagsResults(status); err != nil {
		return
	}
	if withValue {
		mr.Value = make([]byte, size+len(crlf))
		if _, err = io.ReadFull(r, mr.Value); err != nil {
			return
		}
		mr.Value = mr.Value[:size]
	}
	return
}
