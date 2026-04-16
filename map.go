package node

import (
	"context"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode

	state      MapState
	counter    uint64
	allNodeIDs []string
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	return &CRDTMapNode{
		BaseNode:   hive.NewBaseNode(id),
		state:      make(MapState),
		counter:    0,
		allNodeIDs: allNodeIDs,
	}
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	n.BaseNode.Start(ctx)
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		nodeCtx := n.Context()
		for {
			select {
			case <-nodeCtx.Done():
				return
			case <-ticker.C:
				snapshot := n.State()
				for _, peer := range n.allNodeIDs {
					if peer == n.ID() {
						continue
					}
					_ = n.Send(peer, snapshot)
				}
			}
		}
	}()

	return nil
}

func (n *CRDTMapNode) nextVersion() Version {
	n.counter++
	return Version{
		Counter: n.counter,
		NodeID:  n.ID(),
	}
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.state[k] = StateEntry{
		Value:     v,
		Tombstone: false,
		Version:   n.nextVersion(),
	}
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	val, ok := n.ToMap()[k]
	return val, ok
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.state[k] = StateEntry{
		Value:     "",
		Tombstone: true,
		Version:   n.nextVersion(),
	}
}

func (v Version) less(other Version) bool {
	if v.Counter != other.Counter {
		return v.Counter < other.Counter
	}
	return v.NodeID < other.NodeID
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	for key, remoteEntry := range remote {
		localEntry, exists := n.state[key]
		if !exists || localEntry.Version.less(remoteEntry.Version) {
			n.state[key] = remoteEntry
		}
	}
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	snapshot := make(MapState, len(n.state))
	for k, v := range n.state {
		snapshot[k] = v
	}
	return n.state
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	result := make(map[string]string)
	for k, entry := range n.state {
		if !entry.Tombstone {
			result[k] = entry.Value
		}
	}
	return result
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	n.Merge(msg.Payload.(MapState))
	return nil
}
