package main

import (
	"errors"
	"fmt"
)

const (
	resizingWork  int = 128
	maxLoadFactor int = 8
)

type Comparer func(first *HNode, second *HNode) bool

type HNode struct {
	next  *HNode
	hCode uint64
	key   string
	value string
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



type gMap struct {
	db *HMap
}


func newNode(key, value string) *HNode {
	return &HNode{
		next: nil,
		hCode: hashFunction(key),
		key: key,
		value: value,
	}
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
	return (first.key == second.key && first.hCode==second.hCode)
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
	if table == nil {
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




func (table *HTable) detachNode(node *HNode) *HNode {
	pos := node.hCode & uint64(table.mask)
	if table.tab[pos] == nil {
		return nil
	}

	// check the first node
	headNode := table.tab[pos]
	if nodeComparer(headNode, node){
		table.tab[pos] = headNode.next
		table.size --
		return headNode
	}


	// look for the node in the linked list
	previousNode := table.tab[pos]
	for currentNode := previousNode.next; currentNode != nil; currentNode = currentNode.next {
		if nodeComparer(currentNode, node) {
			previousNode.next = currentNode.next
			table.size --
			return currentNode
		}
		previousNode = currentNode
	}
	return nil
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
	fmt.Printf("node: %v found in the first table %v:\n", key, hm.firstTab)
	if from != nil {
		return hm.firstTab.detachNode(from)
	}
	from = hm.secondTab.lookupNode(key, comparer)
	if from != nil {
		return hm.secondTab.detachNode(from)
	}
	return nil

}


func hashFunction(key string) (hash uint64) {
    hash = 0
    for _, ch := range key {
        hash += uint64(ch)
        hash += hash << 10
        hash ^= hash >> 6
    }

    hash += hash << 3
    hash ^= hash >> 11
    hash += hash << 15

    return
}
