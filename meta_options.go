package memcache

type casToken struct {
	value  int64
	setted bool
}

type MetaGetOptions struct {
	BinaryKey        bool // interpret key as base64 encoded binary value
	ReturnCasToken   bool // return item cas token
	ReturnFlags      bool // return client flags token
	ReturnHit        bool // return whether item has been hit before as a 0 or 1
	ReturnLastAccess bool // return time since item was last accessed in seconds
	ReturnSize       bool // return item size token
	ReturnTTL        bool // return item TTL remaining in seconds (-1 for unlimited)
	ReturnValue      bool // return item value in <data block>
	NoBump           bool // don't bump the item in the LRU

	SetTTL                int64  // update remaining TTL
	NewWithTTL            int64  // vivify on miss, takes TTL as a argument
	RequestRecacheWithTTL uint64 // if token is less than remaining TTL win for recache
}

func marshalMGOptions(mgo MetaGetOptions) (fs []metaFlager) {
	if mgo.NewWithTTL != 0 {
		fs = append(fs, withVivify(mgo.NewWithTTL))
	}
	if mgo.BinaryKey {
		fs = append(fs, withBinary())
	}
	if mgo.ReturnCasToken {
		fs = append(fs, withCAS())
	}
	if mgo.ReturnFlags {
		fs = append(fs, withFlag())
	}
	if mgo.ReturnHit {
		fs = append(fs, withHit())
	}
	if mgo.ReturnLastAccess {
		fs = append(fs, withLastAccess())
	}
	if mgo.ReturnSize {
		fs = append(fs, withSize())
	}
	if mgo.ReturnTTL {
		fs = append(fs, withTTL())
	}
	if mgo.ReturnValue {
		fs = append(fs, withValue())
	}
	if mgo.NoBump {
		fs = append(fs, withNoBump())
	}
	if mgo.SetTTL != 0 {
		fs = append(fs, withSetTTL(mgo.SetTTL))
	}
	if mgo.RequestRecacheWithTTL != 0 {
		fs = append(fs, withRecache(mgo.RequestRecacheWithTTL))
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
	BinaryKey      bool        // interpret key as base64 encoded binary value (see metaget)
	ReturnCasToken bool        // return CAS value if successfully stored.
	SetFlag        uint32      // set client flags to token (32 bit unsigned numeric)
	SetInvalidate  bool        // set-to-invalid if supplied CAS is older than item's CAS
	Mode           MetaSetMode // mode switch to change behavior to add, replace, append, prepend
	SetTTL         int64       // Time-To-Live for item, see "Expiration" above.
}

func marshalMSOptions(mso MetaSetOptions) (fs []metaFlager) {
	if mso.BinaryKey {
		fs = append(fs, withBinary())
	}
	if mso.ReturnCasToken {
		fs = append(fs, withCAS())
	}
	if mso.SetFlag != 0 {
		fs = append(fs, withSetFlag(mso.SetFlag))
	}
	if mso.SetInvalidate {
		fs = append(fs, withSetInvalid())
	}
	if mso.Mode != MetaSetModeEmpty {
		fs = append(fs, withMode(string(mso.Mode)))
	}
	if mso.SetTTL != 0 {
		fs = append(fs, withSetTTL(mso.SetTTL))
	}
	if mso.CasToken.setted {
		fs = append(fs, withCompareCAS(mso.CasToken.value))
	}
	return
}

type MetaDeletOptions struct {
	CasToken      casToken // compare and swap token
	BinaryKey     bool     // interpret key as base64 encoded binary value (see metaget)
	SetInvalidate bool     // mark as stale, bumps CAS.
	SetTTL        int64    // updates TTL, only when paired with the SetInvalidate option
}

func marshalMDOptions(mdo MetaDeletOptions) (fs []metaFlager) {
	if mdo.BinaryKey {
		fs = append(fs, withBinary())
	}
	if mdo.SetInvalidate {
		fs = append(fs, withSetInvalid())
	}
	if mdo.SetTTL != 0 {
		fs = append(fs, withSetTTL(mdo.SetTTL))
	}
	if mdo.CasToken.setted {
		fs = append(fs, withCompareCAS(mdo.CasToken.value))
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
	BinaryKey      bool     // interpret key as base64 encoded binary value (see metaget)
	ReturnCasToken bool     // return current CAS value if successful.
	ReturnTTL      bool     // return current TTL
	ReturnValue    bool     // return new value

	SetTTL       int64              // update TTL on success
	NewWithTTL   int64              // auto create item on miss with supplied TTL
	InitialValue uint64             // initial value to use if auto created after miss (default 0)
	Delta        uint64             //  delta to apply (decimal unsigned 64-bit number, default 1)
	Mode         MetaArithmeticMode // mode switch to change between incr and decr modes.
}

func marshalMAOptions(mao MetaArithmeticOptions) (fs []metaFlager) {
	if mao.NewWithTTL != 0 {
		fs = append(fs, withVivify(mao.NewWithTTL))
	}
	if mao.BinaryKey {
		fs = append(fs, withBinary())
	}
	if mao.ReturnCasToken {
		fs = append(fs, withCAS())
	}
	if mao.Delta != 0 {
		fs = append(fs, withDelta(mao.Delta))
	}
	if mao.InitialValue != 0 {
		fs = append(fs, withInitialValue(mao.InitialValue))
	}
	if mao.ReturnTTL {
		fs = append(fs, withTTL())
	}
	if mao.ReturnValue {
		fs = append(fs, withValue())
	}
	if mao.Mode != MetaArithmeticModeEmpty {
		fs = append(fs, withMode(string(mao.Mode)))
	}
	if mao.SetTTL != 0 {
		fs = append(fs, withSetTTL(mao.SetTTL))
	}
	if mao.CasToken.setted {
		fs = append(fs, withCompareCAS(mao.CasToken.value))
	}
	return
}
