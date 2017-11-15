// Package mapstate implements the State interface for IPFS Cluster by using
// a map to keep track of the consensus-shared state.
package mapstate

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"

	msgpack "github.com/multiformats/go-multicodec/msgpack"

	"github.com/ipfs/ipfs-cluster/api"

	cid "github.com/ipfs/go-cid"
)

// Version is the map state Version. States with old versions should
// perform an upgrade before.
const Version = 2

// MapState is a very simple database to store the state of the system
// using a Go map. It is thread safe. It implements the State interface.
type MapState struct {
	pinMux    sync.RWMutex
	toRestore []byte
	PinMap    map[string]api.PinSerial
	Version   int
}

// NewMapState initializes the internal map and returns a new MapState object.
func NewMapState() *MapState {
	return &MapState{
		PinMap:  make(map[string]api.PinSerial),
		Version: Version,
	}
}

// Add adds a Pin to the internal map.
func (st *MapState) Add(c api.Pin) error {
	st.pinMux.Lock()
	defer st.pinMux.Unlock()
	st.PinMap[c.Cid.String()] = c.ToSerial()
	return nil
}

// Rm removes a Cid from the internal map.
func (st *MapState) Rm(c *cid.Cid) error {
	st.pinMux.Lock()
	defer st.pinMux.Unlock()
	delete(st.PinMap, c.String())
	return nil
}

// Get returns Pin information for a CID.
func (st *MapState) Get(c *cid.Cid) api.Pin {
	st.pinMux.RLock()
	defer st.pinMux.RUnlock()
	pins, ok := st.PinMap[c.String()]
	if !ok { // make sure no panics
		return api.Pin{}
	}
	return pins.ToPin()
}

// Has returns true if the Cid belongs to the State.
func (st *MapState) Has(c *cid.Cid) bool {
	st.pinMux.RLock()
	defer st.pinMux.RUnlock()
	_, ok := st.PinMap[c.String()]
	return ok
}

// List provides the list of tracked Pins.
func (st *MapState) List() []api.Pin {
	st.pinMux.RLock()
	defer st.pinMux.RUnlock()
	cids := make([]api.Pin, 0, len(st.PinMap))
	for _, v := range st.PinMap {
		if v.Cid == "" {
			continue
		}
		cids = append(cids, v.ToPin())
	}
	return cids
}

// Snapshot dumps the MapState to the given writer, in pretty json
// format.
func (st *MapState) Snapshot(w io.Writer) error {
	st.pinMux.RLock()
	defer st.pinMux.RUnlock()
	enc := json.NewEncoder(w)
	enc.SetIndent("", "    ")
	return enc.Encode(st)
}

// Restore restores a snapshot from the state's internal bytes. It should
// migrate the format if it is not compatible with the current version.
func (st *MapState) Restore() error {
	return st.migrateFrom(st.Version, st.toRestore)
}


// GetVersion returns the current version of this state object.
// It is not necessarily up to date
func (st *MapState) GetVersion() int {
	return st.Version
}

// GetLatestVersion returns the most up to date version of state
func (st *MapState) GetLatestVersion() int {
	return Version
}


// Marshal encodes the state using msgpack
func (st *MapState) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := msgpack.Multicodec(msgpack.DefaultMsgpackHandle()).Encoder(buf)
	if err := enc.Encode(st); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal decodes the state using msgpack.  It first decodes just
// the version number.  If this is not the current version the bytes
// are stored within the state's internal reader, which can be migrated
// to the current version in a later call to restore.  Note: Out of date
// version is not an error
func (st *MapState) Unmarshal(bs []byte) error {
	bsV := make([]byte, len(bs))
	copy(bsV, bs)
	bufV := bytes.NewBuffer(bsV)
	var vonly struct{ Version int }
	decV := msgpack.Multicodec(msgpack.DefaultMsgpackHandle()).Decoder(bufV)
	if err := decV.Decode(&vonly); err != nil {
		return err
	}
	if vonly.Version != Version { // snapshot is out of date
		st.Version = vonly.Version
		st.toRestore = bs
		return nil
	}

	// snapshot is up to date
	buf := bytes.NewBuffer(bs)
	dec := msgpack.Multicodec(msgpack.DefaultMsgpackHandle()).Decoder(buf)
	if err := dec.Decode(st); err != nil {
		return err
	}
	return nil
}
