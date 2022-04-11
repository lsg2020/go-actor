package gactor

import (
	"sync"
)

type systemHandle struct {
	handleMutex    sync.RWMutex
	handleIndex    uint32
	handleSlotSize uint32
	handleSlots    []Actor
	handles        map[Actor]ActorHandle
}

const (
	DEFAULT_SLOT_SIZE = 4
	HANDLE_MASK       = 0xffffffff
)

func (s *systemHandle) handleInit() {
	s.handleIndex = 1
	s.handleSlotSize = DEFAULT_SLOT_SIZE
	s.handleSlots = make([]Actor, s.handleSlotSize)
	s.handles = make(map[Actor]ActorHandle, s.handleSlotSize)
}

func (s *systemHandle) handleRegister(a Actor) ActorHandle {
	s.handleMutex.Lock()
	defer s.handleMutex.Unlock()

check:
	handle := s.handleIndex
	for i := uint32(0); i < s.handleSlotSize; i++ {
		if handle > HANDLE_MASK {
			handle = 1
		}

		hash := handle & (s.handleSlotSize - 1)
		if s.handleSlots[hash] == nil {
			s.handleSlots[hash] = a
			s.handleIndex = handle + 1
			s.handles[a] = ActorHandle(handle)
			return ActorHandle(handle)
		}
		handle++
	}

	newSlots := make([]Actor, s.handleSlotSize*2)
	for i := uint32(0); i < s.handleSlotSize; i++ {
		hash := uint32(s.handles[s.handleSlots[i]]) & (s.handleSlotSize*2 - 1)
		newSlots[hash] = s.handleSlots[i]
	}
	s.handleSlots = newSlots
	s.handleSlotSize *= 2
	goto check
}

func (s *systemHandle) handleRetire(a Actor) (bool, ActorHandle) {
	s.handleMutex.Lock()
	defer s.handleMutex.Unlock()

	handle := s.handles[a]
	hash := uint32(handle) & (s.handleSlotSize - 1)
	aa := s.handleSlots[hash]
	delete(s.handles, a)

	if aa != nil && aa == a {
		s.handleSlots[hash] = nil
		return true, handle
	}
	return false, handle
}

func (s *systemHandle) find(handle ActorHandle) Actor {
	s.handleMutex.RLock()
	defer s.handleMutex.RUnlock()

	hash := uint32(handle) & (s.handleSlotSize - 1)
	a := s.handleSlots[hash]
	if a != nil {
		return a
	}
	return nil
}
