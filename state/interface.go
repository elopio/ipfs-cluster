// Package state holds the interface that any state implementation for
// IPFS Cluster must satisfy.
package state

// State represents the shared state of the cluster and it
import (
	"io"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/ipfs-cluster/api"
)

// State is used by the Consensus component to keep track of
// objects which objects are pinned. This component should be thread safe.
type State interface {
	// Add adds a pin to the State
	Add(api.Pin) error
	// Rm removes a pin from the State
	Rm(*cid.Cid) error
	// List lists all the pins in the state
	List() []api.Pin
	// Has returns true if the state is holding information for a Cid
	Has(*cid.Cid) bool
	// Get returns the information attacthed to this pin
	Get(*cid.Cid) api.Pin
	// Snapshot writes a snapshot of the state to a writer
	Snapshot(w io.Writer) error
	// Restore restores an outdated state to the current version
	Restore() error
	// Check whether version of state in reader has correct version
	GetVersion(r io.Reader) (int, error)
	// Marshal serializes the state to a byte slice
	Marshal() ([]byte, error)
	// Unmarshal deserializes the state from marshaled bytes
	Unmarshal([]byte) error
}
