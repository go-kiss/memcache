package memcache

type casToken struct {
	value  int64
	setted bool
}

type MetaGetOptions struct {
	ReturnCasToken   bool // return item cas token
	ReturnFlags      bool // return client flags token
	ReturnHit        bool // return whether item has been hit before as a 0 or 1
	ReturnLastAccess bool // return time since item was last accessed in seconds
	ReturnSize       bool // return item size token
	ReturnTTL        bool // return item TTL remaining in seconds (-1 for unlimited)
	ReturnValue      bool // return item value in <data block>
	NoBump           bool // don't bump the item in the LRU

	BinaryKey []byte // interpret key as base64 encoded binary value

	SetTTL         uint64 // update remaining TTL
	NewWithTTL     uint64 // vivify on miss, takes TTL as a argument
	RecacheWithTTL uint64 // if token is less than remaining TTL win for recache
}

func (o MetaGetOptions) marshal() (fs []metaFlag) {
	if o.NewWithTTL != 0 {
		fs = append(fs, withVivify(o.NewWithTTL))
	}
	if len(o.BinaryKey) > 0 {
		fs = append(fs, withBinary())
	}
	if o.ReturnCasToken {
		fs = append(fs, withCAS())
	}
	if o.ReturnFlags {
		fs = append(fs, withFlag())
	}
	if o.ReturnHit {
		fs = append(fs, withHit())
	}
	if o.ReturnLastAccess {
		fs = append(fs, withLastAccess())
	}
	if o.ReturnSize {
		fs = append(fs, withSize())
	}
	if o.ReturnTTL {
		fs = append(fs, withTTL())
	}
	if o.ReturnValue {
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
	CasToken       casToken    // compare and swap token
	BinaryKey      []byte      // interpret key as base64 encoded binary value (see metaget)
	ReturnCasToken bool        // return CAS value if successfully stored.
	SetFlag        uint32      // set client flags to token (32 bit unsigned numeric)
	SetInvalidate  bool        // set-to-invalid if supplied CAS is older than item's CAS
	Mode           MetaSetMode // mode switch to change behavior to add, replace, append, prepend
	SetTTL         uint64      // Time-To-Live for item, see "Expiration" above.
}

func (o MetaSetOptions) marshal() (fs []metaFlag) {
	if len(o.BinaryKey) > 0 {
		fs = append(fs, withBinary())
	}
	if o.ReturnCasToken {
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
	if o.CasToken.setted {
		fs = append(fs, withCompareCAS(o.CasToken.value))
	}
	return
}

type MetaDeletOptions struct {
	CasToken      casToken // compare and swap token
	BinaryKey     []byte   // interpret key as base64 encoded binary value (see metaget)
	SetInvalidate bool     // mark as stale, bumps CAS.
	SetTTL        uint64   // updates TTL, only when paired with the SetInvalidate option
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
	if o.CasToken.setted {
		fs = append(fs, withCompareCAS(o.CasToken.value))
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
	CasToken       casToken // compare and swap token
	BinaryKey      []byte   // interpret key as base64 encoded binary value (see metaget)
	ReturnCasToken bool     // return current CAS value if successful.
	ReturnTTL      bool     // return current TTL
	ReturnValue    bool     // return new value

	SetTTL       uint64 // update TTL on success
	NewWithTTL   uint64 // auto create item on miss with supplied TTL
	InitialValue uint64 // initial value to use if auto created after miss (default 0)
	Delta        uint64 //  delta to apply (decimal unsigned 64-bit number, default 1)

	Mode MetaArithmeticMode // mode switch to change between incr and decr modes.
}

func (o MetaArithmeticOptions) marshal() (fs []metaFlag) {
	if o.NewWithTTL != 0 {
		fs = append(fs, withVivify(o.NewWithTTL))
	}
	if len(o.BinaryKey) > 0 {
		fs = append(fs, withBinary())
	}
	if o.ReturnCasToken {
		fs = append(fs, withCAS())
	}
	if o.Delta != 0 {
		fs = append(fs, withDelta(o.Delta))
	}
	if o.InitialValue != 0 {
		fs = append(fs, withInitialValue(o.InitialValue))
	}
	if o.ReturnTTL {
		fs = append(fs, withTTL())
	}
	if o.ReturnValue {
		fs = append(fs, withValue())
	}
	if o.Mode != MetaArithmeticModeEmpty {
		fs = append(fs, withMode(string(o.Mode)))
	}
	if o.SetTTL != 0 {
		fs = append(fs, withSetTTL(o.SetTTL))
	}
	if o.CasToken.setted {
		fs = append(fs, withCompareCAS(o.CasToken.value))
	}
	return
}
