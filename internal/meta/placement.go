package meta

import (
	"github.com/soumic28/dfs/internal/chunk"
	"github.com/soumic28/dfs/internal/meta/dbgen"
)

// selectTargets chooses which nodes should hold a chunk.
//
// Phase 2 placeholder: take live nodes in a stable order derived from the
// chunk id, so placement is at least deterministic and spread rather than
// always hitting the first node. With one node it is trivially that node.
//
// Phase 3 replaces the body with weighted rendezvous hashing — scoring each
// node by hash(chunk_id || node_id) weighted by free capacity, and taking the
// top R. The signature is already the one that needs, so nothing above this
// function has to change when it does.
func selectTargets(id chunk.ID, nodes []dbgen.Node, replicationFactor int) []dbgen.Node {
	if replicationFactor <= 0 {
		replicationFactor = 1
	}
	if len(nodes) == 0 {
		return nil
	}

	// Rotate the (already id-ordered) node list by a value derived from the
	// chunk id. Crude, but deterministic and evenly spread, which is all
	// Phase 2 needs from it.
	start := int(id[0]) % len(nodes)

	out := make([]dbgen.Node, 0, min(replicationFactor, len(nodes)))
	for i := 0; i < len(nodes) && len(out) < replicationFactor; i++ {
		out = append(out, nodes[(start+i)%len(nodes)])
	}
	return out
}
