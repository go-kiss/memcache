package memcache

import (
	"strconv"
	"strings"
)

var responseFlags = []metaFlager{
	useMetaFlag("W", "", func(mr *MetaResult, res string) error {
		mr.IsWon = true
		return nil
	}),
	useMetaFlag("Z", "", func(mr *MetaResult, res string) error {
		mr.IsSentWon = true
		return nil
	}),
	useMetaFlag("X", "", func(mr *MetaResult, res string) error {
		mr.IsStale = true
		return nil
	}),
	withKey(),
	withOpaque(""),
	withCAS(),
	withFlag(),
	withHit(),
	withLastAccess(),
	withSize(),
	withTTL(),
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
		k, v := f[:1], f[1:]
		if ff, ok := globalRegisteredFlags[k]; ok {
			if err = ff.obtain(&mr, v); err != nil {
				return
			}
		}
	}
	mr.CasToken.setted = true
	return
}

func useMetaFlag(name, input string, setFunc func(*MetaResult, string) error) metaFlager {
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

// withBinary - b: interpret key as base64 encoded binary value
func withBinary() metaFlager { return useMetaFlag("b", "", nil) }

// withCAS - c: return item cas token
func withCAS() metaFlager {
	return useMetaFlag("c", "", func(r *MetaResult, d string) (err error) {
		r.CasToken.value, err = strconv.ParseInt(d, 10, 64)
		return
	})
}

// withFlag - f: return client flags token
func withFlag() metaFlager {
	return useMetaFlag("f", "", func(r *MetaResult, d string) error {
		var err error
		v, err := strconv.ParseUint(d, 10, 32)
		if err != nil {
			return err
		}
		r.Flags = uint32(v)
		return nil
	})
}

// withHit - h: return whether item has been hit before as a 0 or 1
func withHit() metaFlager {
	return useMetaFlag("h", "", func(r *MetaResult, d string) error {
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
	return useMetaFlag("k", "", func(r *MetaResult, d string) error {
		r.Key = d
		return nil
	})
}

// withLastAccess - l: return time since item was last accessed in seconds
func withLastAccess() metaFlager {
	return useMetaFlag("l", "", func(r *MetaResult, d string) error {
		var err error
		r.LastAccess, err = strconv.ParseUint(d, 10, 64)
		return err
	})
}

// withOpaque - O(token): opaque value, consumes a token and copies back with response
func withOpaque(token string) metaFlager {
	token = strings.ReplaceAll(token, " ", "_")
	return useMetaFlag("O", token, func(r *MetaResult, d string) error {
		r.Opaque = d
		return nil
	})
}

// WithQuiet - q: use noreply semantics for return codes.
func withQuiet() metaFlager { return useMetaFlag("q", "", nil) }

// withSize - s: return item size token
func withSize() metaFlager {
	return useMetaFlag("s", "", func(r *MetaResult, d string) error {
		var err error
		r.Size, err = strconv.ParseUint(d, 10, 64)
		return err
	})
}

// withTTL - t: return item TTL remaining in seconds (-1 for unlimited)
func withTTL() metaFlager {
	return useMetaFlag("t", "", func(r *MetaResult, d string) error {
		var err error
		r.TTL, err = strconv.ParseInt(d, 10, 64)
		return err
	})
}

// withNoBump - u: don't bump the item in the LRU
func withNoBump() metaFlager {
	return useMetaFlag("u", "", nil)
}

// withValue - v: return item value in <data block>
func withValue() metaFlager {
	return useMetaFlag("v", "", nil)
}

// withVivify - N(token): vivify on miss, takes TTL as a argument
func withVivify(token int64) metaFlager {
	return useMetaFlag("N", strconv.FormatInt(token, 10), nil)
}

// withRecache - R(token): if token is less than remaining TTL win for recache
func withRecache(token uint64) metaFlager {
	return useMetaFlag("R", strconv.FormatUint(token, 10), nil)
}

// withSetTTL - T(token): update remaining TTL
func withSetTTL(token int64) metaFlager {
	return useMetaFlag("T", strconv.FormatInt(token, 10), nil)
}

// withCompareCAS - C(token): compare CAS value when storing item
func withCompareCAS(token int64) metaFlager {
	return useMetaFlag("C", strconv.FormatInt(token, 10), nil)
}

// withSetFlag - F(token): set client flags to token (32 bit unsigned numeric)
func withSetFlag(token uint32) metaFlager {
	return useMetaFlag("F", strconv.FormatUint(uint64(token), 10), nil)
}

// withSetInvalid - I: invalidate. set-to-invalid if supplied CAS is older than item's CAS / - I: invalidate. mark as stale, bumps CAS.
func withSetInvalid() metaFlager {
	return useMetaFlag("I", "", nil)
}

// withMode - M(token): mode switch to change behavior to add, replace, append, prepend
func withMode(token string) metaFlager {
	return useMetaFlag("M", token, nil)
}

// withInitialValue - J(token): initial value to use if auto created after miss (default 0)
func withInitialValue(token uint64) metaFlager {
	return useMetaFlag("J", strconv.FormatUint(token, 10), nil)
}

// withDelta - D(token): delta to apply (decimal unsigned 64-bit number, default 1)
func withDelta(token uint64) metaFlager {
	return useMetaFlag("D", strconv.FormatUint(token, 10), nil)
}
