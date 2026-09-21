package gateway

import (
	"sort"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/registry"
)

// sortedInstances returns instances ordered by ID so round-robin and
// failure exclusion are deterministic regardless of registry map
// iteration order (weighted/random policies are inherently stochastic).
func sortedInstances(in []registry.Instance) []registry.Instance {
	sort.Slice(in, func(i, j int) bool { return in[i].ID < in[j].ID })
	return in
}
