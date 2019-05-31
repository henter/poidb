package d2

type qnode struct {
	dist float64
	box  box
}

type queue struct {
	nodes []qnode
	len   int
	size  int
}

func (q *queue) push(dist float64, box box) {
	if q.nodes == nil {
		q.nodes = make([]qnode, 2)
	} else {
		q.nodes = append(q.nodes, qnode{})
	}
	i := q.len + 1
	j := i / 2
	for i > 1 && q.nodes[j].dist > dist {
		q.nodes[i] = q.nodes[j]
		i = j
		j = j / 2
	}
	q.nodes[i].dist = dist
	q.nodes[i].box = box
	q.len++
}

func (q *queue) peek() qnode {
	if q.len == 0 {
		return qnode{}
	}
	return q.nodes[1]
}

func (q *queue) pop() qnode {
	if q.len == 0 {
		return qnode{}
	}
	n := q.nodes[1]
	q.nodes[1] = q.nodes[q.len]
	q.len--
	var j, k int
	i := 1
	for i != q.len+1 {
		k = q.len + 1
		j = 2 * i
		if j <= q.len && q.nodes[j].dist < q.nodes[k].dist {
			k = j
		}
		if j+1 <= q.len && q.nodes[j+1].dist < q.nodes[k].dist {
			k = j + 1
		}
		q.nodes[i] = q.nodes[k]
		i = k
	}
	return n
}
