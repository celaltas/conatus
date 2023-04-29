package main

import (
	"errors"
	"unsafe"
)

const (
	resizingWork  int = 128
	maxLoadFactor int = 8
)

type Comparer func(first *HNode, second *HNode) bool

type HNode struct {
	next  *HNode
	hCode uint64
}

type HTable struct {
	tab  []*HNode
	mask uint
	size uint
}

type HMap struct {
	firstTab    *HTable
	secondTab   *HTable
	resizingPos uint
}

type Entry struct {
	node  *HNode
	key   string
	value string
}

type gMap struct {
	db *HMap
}

func getEntry(node *HNode) *Entry {
    offset := uintptr(unsafe.Pointer(Entry{}.node)) - uintptr(unsafe.Pointer(&Entry{}))
    entryPtr := unsafe.Pointer(uintptr(unsafe.Pointer(node)) - offset)
    return (*Entry)(entryPtr)
}


func InitMap() *gMap {
	return &gMap{
		db: &HMap{
			nil,
			nil,
			0,
		},
	}
}

func nodeComparer(first *HNode, second *HNode) bool {
	return first == second
}

func initHasTable(n uint) (*HTable, error) {
	if n > 0 && ((n-1)&n != 0) {
		return nil, errors.New("size must be a power of 2")
	}

	return &HTable{
		tab:  make([]*HNode, n, n),
		mask: n - 1,
		size: 0,
	}, nil
}

func (table *HTable) insertNode(node *HNode) {
	pos := node.hCode & uint64(table.mask)
	currentNode := table.tab[pos]
	node.next = currentNode
	table.tab[pos] = node
	table.size++
}

func (table *HTable) lookupNode(key *HNode, comparer Comparer) *HNode {

	if table.size == 0 {
		return nil
	}
	pos := key.hCode & uint64(table.mask)
	headNode := table.tab[pos]
	for headNode != nil {
		if comparer(headNode, key) {
			return headNode
		}
		headNode = headNode.next
	}
	return nil

}

func (table *HTable) detachNode(from *HNode) *HNode {
	node := from
	from = from.next
	table.size--
	return node
}

func (hm *HMap) lookupNode(key *HNode, comparer Comparer) *HNode {
	hm.resizingHelper()
	from := hm.firstTab.lookupNode(key, comparer)
	if from == nil {
		from = hm.secondTab.lookupNode(key, comparer)
	}
	return from
}

func (hm *HMap) resizingHelper() {
	if hm.secondTab == nil {
		return
	}
	done := 0

	for done < resizingWork && hm.secondTab.size > 0 {
		from := hm.secondTab.tab[hm.resizingPos]
		if from == nil {
			hm.resizingPos++
			continue
		}
		hm.firstTab.insertNode(hm.secondTab.detachNode(from))
		done++
	}
	if hm.secondTab.size == 0 {
		hm.secondTab.tab = hm.secondTab.tab[:0]
		hm.secondTab = &HTable{}
	}

}

func (hm *HMap) insert(node *HNode) {
	if hm.firstTab == nil {
		tab, _ := initHasTable(4)
		hm.firstTab = tab
	}

	hm.firstTab.insertNode(node)
	if hm.secondTab == nil {
		loadFactor := hm.firstTab.size / (hm.firstTab.mask + 1)
		if loadFactor >= uint(maxLoadFactor) {
			hm.startResize()
		}
	}
	hm.resizingHelper()
}

func (hm *HMap) startResize() {
	if hm.secondTab == nil {
		return
	}
	hm.secondTab = hm.firstTab
	tab, err := initHasTable((hm.firstTab.mask + 1) * 2)
	if err != nil {
		return
	}
	hm.firstTab = tab
	hm.resizingPos = 0

}

func (hm *HMap) pop(key *HNode, comparer Comparer) *HNode {
	hm.resizingHelper()
	from := hm.firstTab.lookupNode(key, comparer)
	if from != nil {
		return hm.firstTab.detachNode(from)
	}
	from = hm.secondTab.lookupNode(key, comparer)
	if from != nil {
		return hm.secondTab.detachNode(from)
	}
	return nil

}

func hashFunction(key string) uint64 {
	var sum uint64 = 0
	var factor uint64 = 31
	for index, s := range key {
		sum = (sum + (uint64(index) * factor * uint64(s)))
		factor *= factor
	}
	return sum
}
