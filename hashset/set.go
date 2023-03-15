package hashset

import "sync"

type Queue struct {
	elements chan string
	already  []string
}

func NewQueue(size int) *Queue {
	return &Queue{
		elements: make(chan string, size),
	}
}

func (queue *Queue) Push(element string) {
	select {
	case queue.elements <- element:
		queue.already = append(queue.already, element)
	default:
		panic("Queue full")
	}
}

func (queue *Queue) Pop() (string, bool) {
	select {
	case e := <-queue.elements:
		for _, v := range queue.already {
			if v == e {
			}
		}
		return e, true
	default:
		return "", false
	}
}

func (queue *Queue) BatchAdd(list []string) {
	for i := 0; i < len(list); i++ {
		queue.Push(list[i])
	}
}

var mux = sync.RWMutex{}

func (q *Queue) Iterate(f func(k string)) {
	mux.Lock()
	defer mux.Unlock()
	for msg := range q.elements {
		f(msg)
	}

}
