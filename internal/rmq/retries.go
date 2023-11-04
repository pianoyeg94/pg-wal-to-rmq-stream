package rmq

// func newRetry(msg *Msg) *retry {
// 	return &retry{
// 		msg:  msg,
// 		when: time.Now().Unix() + int64(msg.retryDelay.Seconds()),
// 	}
// }

// type retry struct {
// 	msg  *Msg
// 	when int64
// }

// func newRetriesHeap() *retriesHeap {
// 	return &retriesHeap{}
// }

// type retriesHeap struct {
// 	heap []*retry
// 	size int
// }

// func (h *retriesHeap) insert(r *retry) {
// 	if h.size < len(h.heap) {
// 		h.heap[h.size] = r
// 	} else {
// 		h.heap = append(h.heap, r)
// 	}
// 	h.size++

// 	idx := h.size - 1
// 	parentIdx := (idx - 1) / 2
// 	for parentIdx >= 0 && h.heap[idx].when < h.heap[parentIdx].when {
// 		h.heap[idx], h.heap[parentIdx] = h.heap[parentIdx], h.heap[idx]
// 		idx, parentIdx = parentIdx, (parentIdx-1)/2
// 	}
// }

// func (h *retriesHeap) retrieve() *retry {
// 	if len(h.heap) == 0 {
// 		return nil
// 	}

// 	r := h.heap[0]
// 	h.heap[0], h.heap[h.size-1] = h.heap[h.size-1], h.heap[0]
// 	h.heap[len(h.heap)-1] = nil
// 	h.size--

// 	h.heapify()

// 	return r
// }

// func (h *retriesHeap) peek() *retry {
// 	if h.size == 0 {
// 		return nil
// 	}

// 	return h.heap[0]
// }

// func (h *retriesHeap) heapify() {
// 	var idx int
// 	for {
// 		currIdx := idx
// 		leftChildIdx := (2 * idx) + 1
// 		rightChildIdx := (2 * idx) + 2

// 		if leftChildIdx < h.size && h.heap[leftChildIdx].when < h.heap[idx].when {
// 			idx = leftChildIdx
// 		}

// 		if rightChildIdx < h.size && h.heap[rightChildIdx].when < h.heap[idx].when {
// 			idx = rightChildIdx
// 		}

// 		if idx == currIdx {
// 			return
// 		}

// 		h.heap[currIdx], h.heap[idx] = h.heap[idx], h.heap[currIdx]
// 	}
// }
