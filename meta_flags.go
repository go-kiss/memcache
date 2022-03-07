package memcache

import (
	"fmt"
	"strconv"
	"strings"
)

type metaFlag = string

func buildMetaFlags(fs []metaFlag) string {
	return strings.Join(fs, " ")
}

func obtainMetaFlagsResults(ss []string) (mr MetaResult, err error) {
	// Always set the cas token as setted
	// enforce the operation use this token always use the CasToken.value
	// even if the token is not returned.
	// To avoid unexpected non-cas opertion caused by the lack of "c" flag.
	mr.CasToken.setted = true
	for _, f := range ss {
		k, v := f[0], f[1:]
		switch k {
		case 'W':
			mr.Won = true
		case 'Z':
			mr.Won = false
		case 'X':
			mr.Stale = true
		case 'k':
			mr.Key = v
		case 'O':
			mr.Opaque = v
		case 'c':
			mr.CasToken.value, err = strconv.ParseInt(v, 10, 64)
		case 'f':
			v, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return mr, err
			}
			mr.Flags = uint32(v)
		case 'h':
			mr.Hit = (v[0] == '1')
		case 'l':
			mr.LastAccess, err = strconv.ParseUint(v, 10, 64)
		case 's':
			mr.Size, err = strconv.Atoi(v)
		case 't':
			mr.TTL, err = strconv.ParseInt(v, 10, 64)
		default:
			err = fmt.Errorf("Invalid flag: %c", k)
		}
	}
	return
}

// withBinary - b: interpret key as base64 encoded binary value
func withBinary() metaFlag {
	return "b"
}

// withCAS - c: return item cas token
func withCAS() metaFlag {
	return "c"
}

// withFlag - f: return client flags token
func withFlag() metaFlag {
	return "f"
}

// withHit - h: return whether item has been hit before as a 0 or 1
func withHit() metaFlag {
	return "h"
}

// withLastAccess - l: return time since item was last accessed in seconds
func withLastAccess() metaFlag {
	return "l"
}

// withOpaque - O(token): opaque value, consumes a token and copies back with response
func withOpaque(token string) metaFlag {
	return "O" + token
}

// WithQuiet - q: use noreply semantics for return codes.
func withQuiet() metaFlag {
	return "q"
}

// withSize - s: return item size token
func withSize() metaFlag {
	return "s"
}

// withTTL - t: return item TTL remaining in seconds (-1 for unlimited)
func withTTL() metaFlag {
	return "t"
}

// withNoBump - u: don't bump the item in the LRU
func withNoBump() metaFlag {
	return "u"
}

// withValue - v: return item value in <data block>
func withValue() metaFlag {
	return "v"
}

// withVivify - N(token): vivify on miss, takes TTL as a argument
func withVivify(token uint64) metaFlag {
	return "N" + strconv.FormatUint(token, 10)
}

// withRecache - R(token): if token is less than remaining TTL win for recache
func withRecache(token uint64) metaFlag {
	return "R" + strconv.FormatUint(token, 10)
}

// withSetTTL - T(token): update remaining TTL
func withSetTTL(token uint64) metaFlag {
	return "T" + strconv.FormatUint(token, 10)
}

// withCompareCAS - C(token): compare CAS value when storing item
func withCompareCAS(token int64) metaFlag {
	return "C" + strconv.FormatInt(token, 10)
}

// withSetFlag - F(token): set client flags to token (32 bit unsigned numeric)
func withSetFlag(token uint32) metaFlag {
	return "F" + strconv.FormatUint(uint64(token), 10)
}

// withSetInvalid - I: invalidate. set-to-invalid if supplied CAS is older than item's CAS / - I: invalidate. mark as stale, bumps CAS.
func withSetInvalid() metaFlag {
	return "I"
}

// withMode - M(token): mode switch to change behavior to add, replace, append, prepend
func withMode(token string) metaFlag {
	return "M" + token
}

// withInitialValue - J(token): initial value to use if auto created after miss (default 0)
func withInitialValue(token uint64) metaFlag {
	return "J" + strconv.FormatUint(token, 10)
}

// withDelta - D(token): delta to apply (decimal unsigned 64-bit number, default 1)
func withDelta(token uint64) metaFlag {
	return "D" + strconv.FormatUint(token, 10)
}
