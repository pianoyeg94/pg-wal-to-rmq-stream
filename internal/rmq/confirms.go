package rmq

import (
	"sync"
)

func newConfirms() *confirms {
	return &confirms{
		tagsToMsgs: make(map[uint64]*Msg),
	}
}

type confirms struct {
	sync.RWMutex
	tagsToMsgs map[uint64]*Msg
}

func (c *confirms) store(tag uint64, msg *Msg) {
	c.Lock()
	defer c.Unlock()
	c.tagsToMsgs[tag] = msg
}

func (c *confirms) pop(tag uint64) (*Msg, bool) {
	c.RLock()
	defer c.RUnlock()
	if msg, ok := c.tagsToMsgs[tag]; ok {
		delete(c.tagsToMsgs, tag)
		return msg, true
	}

	return nil, false
}

func (c *confirms) forEach(fn func(msg *Msg)) {
	c.Lock()
	defer c.Unlock()
	for _, msg := range c.tagsToMsgs {
		fn(msg)
	}
}

func (c *confirms) len() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.tagsToMsgs)
}
