package main

type AVLNode struct {
	depth  uint32
	cnt    uint32
	left   *AVLNode
	right  *AVLNode
	parent *AVLNode
}

func avlInit() *AVLNode {
	return &AVLNode{
		1,
		1,
		nil,
		nil,
		nil,
	}
}

func avlDepth(node *AVLNode) uint32 {
	if node == nil {
		return 0
	}
	return node.depth
}

func avlCnt(node *AVLNode) uint32 {
	if node == nil {
		return 0
	}
	return node.cnt
}

func max(lhs, rhs uint32) uint32 {
	if lhs > rhs {
		return lhs
	}
	return rhs
}

func avlUpdate(node *AVLNode) {
	node.depth = max(avlDepth(node.left), avlDepth(node.right)) + 1
	node.cnt = avlCnt(node.left) + avlCnt(node.right) + 1

}

func rotateLeft(node *AVLNode) *AVLNode {
	new_node := node.right
	if new_node.left != nil {
		new_node.left.parent = node
	}
	node.right = new_node.left
	new_node.left = node
	new_node.parent = node.parent
	node.parent = new_node
	avlUpdate(node)
	avlUpdate(new_node)
	return new_node
}

func rotateRight(node *AVLNode) *AVLNode {
	new_node := node.left
	if new_node.right != nil {
		new_node.right.parent = node
	}
	node.left = new_node.right
	new_node.right = node
	new_node.parent = node.parent
	node.parent = new_node
	avlUpdate(node)
	avlUpdate(new_node)
	return new_node

}

func avlFixLeft(root *AVLNode) *AVLNode {
	if avlDepth(root.left.left) < avlDepth(root.left.right) {
		root.left = rotateLeft(root.left)
	}
	return rotateRight(root)
}

func avlFixRight(root *AVLNode) *AVLNode {
	if avlDepth(root.right.right) < avlDepth(root.right.left) {
		root.right = rotateRight(root.right)
	}
	return rotateLeft(root)
}

func avlFix(node *AVLNode) *AVLNode {

	for {
		avlUpdate(node)
		l := avlDepth(node.left)
		r := avlDepth(node.right)
		var from *AVLNode = nil
		if node.parent != nil {
			if node.parent.left == node {
				from = node.parent.left
			} else {
				from = node.parent.right
			}
		}
		if l == r+2 {
			node = avlFixLeft(node)
		} else if r == l+2 {
			node = avlFixRight(node)
		}
		if from != nil {
			return node
		}
		from = node
		node = node.parent

	}

}

func avlDelete(node *AVLNode) *AVLNode {
	if node.right == nil {
		parent := node.parent
		if node.left != nil {
			node.left.parent = parent
		}
		if parent != nil {
			if parent.left == node {
				parent.left = node.left
			} else {
				parent.right = node.left
			}
			return avlFix(parent)
		} else {

			return node.left
		}

	} else {
		victim := node.right
		for victim.left != nil {
			victim = victim.left
		}
		root := avlDelete(victim)
		victim = node
		if victim.left != nil {
			victim.left.parent = victim
		}
		if victim.right != nil {
			victim.right.parent = victim
		}
		parent := node.parent

		if parent != nil {

			if parent.left == node {
				parent.left = victim
			} else {
				parent.right = victim
			}
			return root
		} else {
			return victim
		}

	}
}
