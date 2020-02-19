package pool

import (
	"sort"
	"strconv"
	"sync"
)

const NodesPerServer = 4

type node struct {
	name        string
	hash        uint32
	serverIndex int
}

type nodes []*node

func (ns nodes) Len() int           { return len(ns) }
func (ns nodes) Less(i, j int) bool { return ns[i].hash < ns[j].hash }
func (ns nodes) Swap(i, j int)      { ns[i], ns[j] = ns[j], ns[i] }
func (ns nodes) Sort()              { sort.Sort(ns) }

type ring struct {
	fn     func(key []byte) uint32
	nodes  nodes
	length int
	sync.RWMutex
}

func NewRing(fn func(key []byte) uint32) (h *ring) {
	h = &ring{
		fn:    fn,
		nodes: make(nodes, 0),
	}
	return
}

func (h *ring) Add(name string, index int) {
	h.Lock()
	defer h.Unlock()
	for i := 1; i <= NodesPerServer; i++ {
		n := &node{
			name:        name,
			serverIndex: index,
			hash:        h.fn([]byte(name + ":" + strconv.Itoa(i))),
		}
		h.nodes = append(h.nodes, n)
	}
	h.nodes.Sort()
	h.length = len(h.nodes)
}

func (h *ring) Remove(name string) {
	h.Lock()
	defer h.Unlock()
	nodes := make(nodes, 0)
	for i, node := range h.nodes {
		if node.name != name {
			nodes = append(nodes, h.nodes[i])
		}
	}
	h.nodes = nodes
}

func (h *ring) Hash(s string) int {
	h.RLock()
	defer h.RUnlock()
	val := h.fn([]byte(s))
	i := sort.Search(h.length, func(i int) bool { return h.nodes[i].hash >= val })
	if i == h.length {
		i = 0
	}
	return h.nodes[i].serverIndex
}
