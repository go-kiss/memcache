package memcache

type CasToken interface {
	value() int64
}

type casToken int64

func (c casToken) value() int64 {
	return int64(c)
}

type MetaGetOptions struct {
	Key       string // the key of item
	BinaryKey []byte // interpret key as base64 encoded binary value

	GetCasToken   bool // return item cas token
	GetFlags      bool // return client flags token
	GetHit        bool // return whether item has been hit before as a 0 or 1
	GetLastAccess bool // return time since item was last accessed in seconds
	GetSize       bool // return item size token
	GetTTL        bool // return item TTL remaining in seconds (-1 for unlimited)
	GetValue      bool // return item value in <data block>

	SetTTL           uint64 // update remaining TTL
	SetVivifyWithTTL uint64 // vivify on miss, takes TTL as a argument

	RecacheWithTTL uint64 // if token is less than remaining TTL win for recache
	NoBump         bool   // don't bump the item in the LRU
}

func (o MetaGetOptions) marshal() (fs []metaFlag) {
	if o.SetVivifyWithTTL != 0 {
		fs = append(fs, withVivify(o.SetVivifyWithTTL))
	}
	if len(o.BinaryKey) > 0 {
		fs = append(fs, withBinary())
	}
	if o.GetCasToken {
		fs = append(fs, withCAS())
	}
	if o.GetFlags {
		fs = append(fs, withFlag())
	}
	if o.GetHit {
		fs = append(fs, withHit())
	}
	if o.GetLastAccess {
		fs = append(fs, withLastAccess())
	}
	if o.GetSize {
		fs = append(fs, withSize())
	}
	if o.GetTTL {
		fs = append(fs, withTTL())
	}
	if o.GetValue {
		fs = append(fs, withValue())
	}
	if o.NoBump {
		fs = append(fs, withNoBump())
	}
	if o.SetTTL != 0 {
		fs = append(fs, withSetTTL(o.SetTTL))
	}
	if o.RecacheWithTTL != 0 {
		fs = append(fs, withRecache(o.RecacheWithTTL))
	}
	return
}

type MetaSetMode string

const (
	MetaSetModeEmpty   MetaSetMode = ""
	MetaSetModeAdd     MetaSetMode = "E"
	MetaSetModeAppend  MetaSetMode = "A"
	MetaSetModePrepend MetaSetMode = "P"
	MetaSetModeReplace MetaSetMode = "R"
	MetaSetModeSet     MetaSetMode = "S"
)

type MetaSetOptions struct {
	Key       string   // the key of item
	BinaryKey []byte   // interpret key as base64 encoded binary value (see metaget)
	Value     []byte   // the value of item
	CasToken  CasToken // compare and swap token

	GetCasToken bool // return CAS value if successfully stored.

	SetTTL        uint64 // Time-To-Live for item, see "Expiration" above.
	SetFlag       uint32 // set client flags to token (32 bit unsigned numeric)
	SetInvalidate bool   // set-to-invalid if supplied CAS is older than item's CAS

	Mode MetaSetMode // mode switch to change behavior to add, replace, append, prepend
}

func (o MetaSetOptions) marshal() (fs []metaFlag) {
	if len(o.BinaryKey) > 0 {
		fs = append(fs, withBinary())
	}
	if o.GetCasToken {
		fs = append(fs, withCAS())
	}
	if o.SetFlag != 0 {
		fs = append(fs, withSetFlag(o.SetFlag))
	}
	if o.SetInvalidate {
		fs = append(fs, withSetInvalid())
	}
	if o.Mode != MetaSetModeEmpty {
		fs = append(fs, withMode(string(o.Mode)))
	}
	if o.SetTTL != 0 {
		fs = append(fs, withSetTTL(o.SetTTL))
	}
	if o.CasToken != nil {
		fs = append(fs, withCompareCAS(o.CasToken.value()))
	}
	return
}

type MetaDeletOptions struct {
	Key       string   // the key of item
	BinaryKey []byte   // interpret key as base64 encoded binary value (see metaget)
	CasToken  CasToken // compare and swap token

	SetTTL        uint64 // updates TTL, only when paired with the SetInvalidate option
	SetInvalidate bool   // mark as stale, bumps CAS.
}

func (o MetaDeletOptions) marshal() (fs []metaFlag) {
	if len(o.BinaryKey) > 0 {
		fs = append(fs, withBinary())
	}
	if o.SetInvalidate {
		fs = append(fs, withSetInvalid())
	}
	if o.SetTTL != 0 {
		fs = append(fs, withSetTTL(o.SetTTL))
	}
	if o.CasToken != nil {
		fs = append(fs, withCompareCAS(o.CasToken.value()))
	}
	return
}

type MetaArithmeticMode string

const (
	MetaArithmeticModeEmpty     MetaArithmeticMode = ""
	MetaArithmeticModeIncrement MetaArithmeticMode = "I"
	MetaArithmeticModeDecrement MetaArithmeticMode = "D"
)

type MetaArithmeticOptions struct {
	Key       string   // the key of item
	BinaryKey []byte   // interpret key as base64 encoded binary value (see metaget)
	CasToken  CasToken // compare and swap token

	GetCasToken bool // return current CAS value if successful.
	GetTTL      bool // return current TTL
	GetValue    bool // return new value

	SetTTL           uint64 // update TTL on success
	SetVivifyWithTTL uint64 // auto create item on miss with supplied TTL

	InitialValue uint64             // initial value to use if auto created after miss (default 0)
	Delta        uint64             //  delta to apply (decimal unsigned 64-bit number, default 1)
	Mode         MetaArithmeticMode // mode switch to change between incr and decr modes.
}

func (o MetaArithmeticOptions) marshal() (fs []metaFlag) {
	if o.SetVivifyWithTTL != 0 {
		fs = append(fs, withVivify(o.SetVivifyWithTTL))
	}
	if len(o.BinaryKey) > 0 {
		fs = append(fs, withBinary())
	}
	if o.GetCasToken {
		fs = append(fs, withCAS())
	}
	if o.Delta != 0 {
		fs = append(fs, withDelta(o.Delta))
	}
	if o.InitialValue != 0 {
		fs = append(fs, withInitialValue(o.InitialValue))
	}
	if o.GetTTL {
		fs = append(fs, withTTL())
	}
	if o.GetValue {
		fs = append(fs, withValue())
	}
	if o.Mode != MetaArithmeticModeEmpty {
		fs = append(fs, withMode(string(o.Mode)))
	}
	if o.SetTTL != 0 {
		fs = append(fs, withSetTTL(o.SetTTL))
	}
	if o.CasToken != nil {
		fs = append(fs, withCompareCAS(o.CasToken.value()))
	}
	return
}
