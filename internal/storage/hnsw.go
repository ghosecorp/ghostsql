// internal/storage/hnsw.go
package storage

import (
	"container/heap"
	"math"
	"math/rand"
)

// HNSWIndex implements Hierarchical Navigable Small World graph for vector search
type HNSWIndex struct {
	Vectors        []*Vector
	RowIDs         []int
	Graph          []map[int][]int // graph[layer][nodeID] = []neighborIDs
	EntryPoint     int
	MaxLayers      int
	M              int // Max connections per node
	EfConstruction int // Size of dynamic candidate list
	Metric         VectorDistance
}

// NewHNSWIndex creates a new HNSW index
func NewHNSWIndex(m, efConstruction int, metric VectorDistance) *HNSWIndex {
	return &HNSWIndex{
		Vectors:        make([]*Vector, 0),
		RowIDs:         make([]int, 0),
		Graph:          make([]map[int][]int, 0),
		EntryPoint:     -1,
		MaxLayers:      16,
		M:              m,
		EfConstruction: efConstruction,
		Metric:         metric,
	}
}

// Add adds a vector to the HNSW index
func (h *HNSWIndex) Add(vec *Vector, rowID int) error {
	id := len(h.Vectors)
	h.Vectors = append(h.Vectors, vec)
	h.RowIDs = append(h.RowIDs, rowID)

	// Determine layer for this node
	layer := h.randomLayer()

	// Ensure we have enough layers
	for len(h.Graph) <= layer {
		h.Graph = append(h.Graph, make(map[int][]int))
	}

	// Initialize node in all layers
	for l := 0; l <= layer; l++ {
		if h.Graph[l] == nil {
			h.Graph[l] = make(map[int][]int)
		}
		h.Graph[l][id] = make([]int, 0)
	}

	// If this is the first vector
	if h.EntryPoint == -1 {
		h.EntryPoint = id
		return nil
	}

	// Find nearest neighbors and create connections
	ep := h.EntryPoint
	for l := len(h.Graph) - 1; l > layer; l-- {
		ep = h.findClosestInLayer(vec, ep, l)
	}

	for l := layer; l >= 0; l-- {
		candidates := h.searchLayer(vec, ep, h.EfConstruction, l)
		h.connectNeighbors(id, candidates, l)
		if len(candidates) > 0 {
			ep = candidates[0].ID
		}
	}

	return nil
}

// Search performs k-NN search
func (h *HNSWIndex) Search(query *Vector, k int, ef int) ([]VectorSearchResult, error) {
	if h.EntryPoint == -1 {
		return []VectorSearchResult{}, nil
	}

	// Search from top layer to bottom
	ep := h.EntryPoint
	for l := len(h.Graph) - 1; l > 0; l-- {
		ep = h.findClosestInLayer(query, ep, l)
	}

	// Search in layer 0 with larger candidate list
	candidates := h.searchLayer(query, ep, ef, 0)

	// Convert to results
	results := make([]VectorSearchResult, 0, k)
	for i := 0; i < len(candidates) && i < k; i++ {
		if candidates[i].ID < len(h.RowIDs) {
			results = append(results, VectorSearchResult{
				Row:      Row{"_row_id": h.RowIDs[candidates[i].ID]},
				Distance: candidates[i].Distance,
			})
		}
	}

	return results, nil
}

func (h *HNSWIndex) randomLayer() int {
	layer := 0
	ml := 1.0 / math.Log(float64(h.M))
	for layer < h.MaxLayers && rand.Float64() < math.Exp(-float64(layer)/ml) {
		layer++
	}
	return layer
}

func (h *HNSWIndex) findClosestInLayer(query *Vector, ep int, layer int) int {
	if layer >= len(h.Graph) {
		return ep
	}

	closest := ep
	closestDist, _ := CalculateDistance(query, h.Vectors[closest], h.Metric)

	changed := true
	for changed {
		changed = false
		neighbors, exists := h.Graph[layer][closest]
		if !exists {
			break
		}

		for _, neighborID := range neighbors {
			if neighborID >= len(h.Vectors) {
				continue
			}
			dist, err := CalculateDistance(query, h.Vectors[neighborID], h.Metric)
			if err != nil {
				continue
			}

			if dist < closestDist {
				closest = neighborID
				closestDist = dist
				changed = true
			}
		}
	}

	return closest
}

type Neighbor struct {
	ID       int
	Distance float64
}

func (h *HNSWIndex) searchLayer(query *Vector, ep int, ef int, layer int) []Neighbor {
	if layer >= len(h.Graph) {
		return []Neighbor{}
	}

	visited := make(map[int]bool)
	candidates := make(PriorityQueue, 0)
	heap.Init(&candidates)

	// Add entry point
	dist, _ := CalculateDistance(query, h.Vectors[ep], h.Metric)
	heap.Push(&candidates, &Item{
		ID:       ep,
		Distance: dist,
	})
	visited[ep] = true

	results := make([]Neighbor, 0)

	for candidates.Len() > 0 {
		current := heap.Pop(&candidates).(*Item)

		if len(results) >= ef {
			break
		}

		results = append(results, Neighbor{
			ID:       current.ID,
			Distance: current.Distance,
		})

		// Explore neighbors
		neighbors, exists := h.Graph[layer][current.ID]
		if exists {
			for _, neighborID := range neighbors {
				if !visited[neighborID] && neighborID < len(h.Vectors) {
					visited[neighborID] = true
					dist, err := CalculateDistance(query, h.Vectors[neighborID], h.Metric)
					if err == nil {
						heap.Push(&candidates, &Item{
							ID:       neighborID,
							Distance: dist,
						})
					}
				}
			}
		}
	}

	return results
}

func (h *HNSWIndex) connectNeighbors(id int, candidates []Neighbor, layer int) {
	m := h.M
	if layer == 0 {
		m = h.M * 2
	}

	// Select M best candidates
	selected := candidates
	if len(selected) > m {
		selected = selected[:m]
	}

	// Add bidirectional connections
	for _, neighbor := range selected {
		// Add neighbor to current node
		h.Graph[layer][id] = append(h.Graph[layer][id], neighbor.ID)

		// Add current node to neighbor
		if _, exists := h.Graph[layer][neighbor.ID]; !exists {
			h.Graph[layer][neighbor.ID] = make([]int, 0)
		}
		h.Graph[layer][neighbor.ID] = append(h.Graph[layer][neighbor.ID], id)

		// Prune if too many connections
		if len(h.Graph[layer][neighbor.ID]) > m {
			h.Graph[layer][neighbor.ID] = h.Graph[layer][neighbor.ID][:m]
		}
	}
}

// Priority queue for HNSW search
type Item struct {
	ID       int
	Distance float64
	index    int
}

type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Distance < pq[j].Distance
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}
