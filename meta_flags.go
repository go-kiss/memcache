package memcache

import (
	"strconv"
	"strings"
)

var responseFlags = []metaFlager{
	MetaFlag("W", "", func(mr *MetaResult, res string) error {
		mr.IsWon = true
		return nil
	}),
	MetaFlag("Z", "", func(mr *MetaResult, res string) error {
		mr.IsSentWon = true
		return nil
	}),
	MetaFlag("X", "", func(mr *MetaResult, res string) error {
		mr.IsStale = true
		return nil
	}),
	withKey(),
	withOpaque(""),
	WithCAS(),
	WithFlag(),
	WithHit(),
	WithLastAccess(),
	WithSize(),
	WithTTL(),
}

var globalRegisteredFlags = map[string]metaFlager{}

func init() {
	for _, f := range responseFlags {
		globalRegisteredFlags[f.getKey()] = f
	}
}

type metaFlager interface {
	build([]string) []string
	obtain(*MetaResult, string) error
	getKey() string
}

func buildMetaFlags(fs []metaFlager) string {
	ss := make([]string, 0, len(fs))
	for _, f := range fs {
		ss = f.build(ss)
	}
	return strings.Join(ss, " ")
}

func obtainMetaFlagsResults(ss []string) (mr MetaResult, err error) {
	for _, f := range ss {
		if len(f) == 0 {
			continue
		}
		r := []rune(f)
		k, v := string(r[:1]), string(r[1:])
		if ff, ok := globalRegisteredFlags[k]; ok {
			if err = ff.obtain(&mr, v); err != nil {
				return
			}
		}
	}
	mr.CasToken.setted = true
	return
}

func MetaFlag(name, input string, setFunc func(*MetaResult, string) error) metaFlager {
	return metaFlag{
		k:   name,
		i:   input,
		set: setFunc,
	}
}

type metaFlag struct {
	k   string // key
	i   string // input
	set func(*MetaResult, string) error
}

func (f metaFlag) build(of []string) []string {
	return append(of, f.k+f.i)
}

func (f metaFlag) obtain(mr *MetaResult, input string) error {
	if f.set == nil {
		return nil
	}
	return f.set(mr, input)
}

func (f metaFlag) getKey() string {
	return f.k
}

// WithBinary - b: interpret key as base64 encoded binary value
func WithBinary() metaFlager { return MetaFlag("b", "", nil) }

// WithCAS - c: return item cas token
func WithCAS() metaFlager {
	return MetaFlag("c", "", func(r *MetaResult, d string) error {
		v, err := strconv.ParseInt(d, 10, 64)
		if err != nil {
			return err
		}
		r.CasToken = CasToken(v)
		return nil
	})
}

// WithFlag - f: return client flags token
func WithFlag() metaFlager {
	return MetaFlag("f", "", func(r *MetaResult, d string) error {
		var err error
		v, err := strconv.ParseUint(d, 10, 32)
		if err != nil {
			return err
		}
		r.Flags = uint32(v)
		return nil
	})
}

// WithHit - h: return whether item has been hit before as a 0 or 1
func WithHit() metaFlager {
	return MetaFlag("h", "", func(r *MetaResult, d string) error {
		var err error
		v, err := strconv.ParseUint(d, 10, 32)
		if err != nil {
			return err
		}
		r.Hit = uint32(v)
		return nil
	})
}

// withKey - k: return key as a token
func withKey() metaFlager {
	return MetaFlag("k", "", func(r *MetaResult, d string) error {
		r.Key = d
		return nil
	})
}

// WithLastAccess - l: return time since item was last accessed in seconds
func WithLastAccess() metaFlager {
	return MetaFlag("l", "", func(r *MetaResult, d string) error {
		var err error
		r.LastAccess, err = strconv.ParseUint(d, 10, 64)
		return err
	})
}

// withOpaque - O(token): opaque value, consumes a token and copies back with response
func withOpaque(token string) metaFlager {
	token = strings.ReplaceAll(token, " ", "_")
	return MetaFlag("O", token, func(r *MetaResult, d string) error {
		r.Opaque = d
		return nil
	})
}

// WithQuiet - q: use noreply semantics for return codes.
func withQuiet() metaFlager { return MetaFlag("q", "", nil) }

// WithSize - s: return item size token
func WithSize() metaFlager {
	return MetaFlag("s", "", func(r *MetaResult, d string) error {
		var err error
		r.Size, err = strconv.ParseUint(d, 10, 64)
		return err
	})
}

// WithTTL - t: return item TTL remaining in seconds (-1 for unlimited)
func WithTTL() metaFlager {
	return MetaFlag("t", "", func(r *MetaResult, d string) error {
		var err error
		r.TTL, err = strconv.ParseInt(d, 10, 64)
		return err
	})
}

// WithNoBump - u: don't bump the item in the LRU
func WithNoBump() metaFlager {
	return MetaFlag("u", "", nil)
}

// WithValue - v: return item value in <data block>
func WithValue() metaFlager {
	return MetaFlag("v", "", nil)
}

// WithVivify - N(token): vivify on miss, takes TTL as a argument
func WithVivify(token int64) metaFlager {
	return MetaFlag("N", strconv.FormatInt(token, 10), nil)
}

// WithRecache - R(token): if token is less than remaining TTL win for recache
func WithRecache(token uint64) metaFlager {
	return MetaFlag("R", strconv.FormatUint(token, 10), nil)
}

// WithSetTTL - T(token): update remaining TTL
func WithSetTTL(token int64) metaFlager {
	return MetaFlag("T", strconv.FormatInt(token, 10), nil)
}

// WithCompareCAS - C(token): compare CAS value when storing item
func WithCompareCAS(token int64) metaFlager {
	return MetaFlag("C", strconv.FormatInt(token, 10), nil)
}

// WithSetFlag - F(token): set client flags to token (32 bit unsigned numeric)
func WithSetFlag(token uint32) metaFlager {
	return MetaFlag("F", strconv.FormatUint(uint64(token), 10), nil)
}

// WithSetInvalid - I: invalidate. set-to-invalid if supplied CAS is older than item's CAS / - I: invalidate. mark as stale, bumps CAS.
func WithSetInvalid() metaFlager {
	return MetaFlag("I", "", nil)
}

// WithMode - M(token): mode switch to change behavior to add, replace, append, prepend
func WithMode(token string) metaFlager {
	return MetaFlag("M", token, nil)
}

// WithInitialValue - J(token): initial value to use if auto created after miss (default 0)
func WithInitialValue(token uint64) metaFlager {
	return MetaFlag("J", strconv.FormatUint(token, 10), nil)
}

// WithDelta - D(token): delta to apply (decimal unsigned 64-bit number, default 1)
func WithDelta(token uint64) metaFlager {
	return MetaFlag("D", strconv.FormatUint(token, 10), nil)
}
