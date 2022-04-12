package gactor

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"
)

type Selector interface {
	Addr() *ActorAddr
	Close()
}

func NewRandomSelector(system *ActorSystem, name string) (Selector, error) {
	selector, err := newEtcdSelector(system, name)
	if err != nil {
		return nil, err
	}

	return selector, nil
}

func NewLoopSelector(system *ActorSystem, name string) (Selector, error) {
	selector, err := newEtcdSelector(system, name)
	if err != nil {
		return nil, err
	}

	return &loopSelector{etcd: selector}, nil
}

func newEtcdSelector(system *ActorSystem, name string) (*etcdSelector, error) {
	preFixName := fmt.Sprintf("/%s/%s/names/%s/", system.options.etcdPrefix, system.options.name, name)
	rsp, err := system.etcdClient.Get(system.Context(), preFixName, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(system.Context())
	selector := &etcdSelector{
		cancel: cancel,
	}

	addAddr := func(val []byte) {
		addr := &ActorAddr{}
		err := json.Unmarshal(val, addr)
		if err != nil {
			system.Logger().Errorf("select unmarshal error path:%s val:%s", preFixName, string(val))
			return
		}
		selector.addrMutex.Lock()
		defer selector.addrMutex.Unlock()
		selector.addrList = append(selector.addrList, addr)
	}

	delAddr := func(val []byte) {
		addr := &ActorAddr{}
		err := json.Unmarshal(val, addr)
		if err != nil {
			system.Logger().Errorf("select unmarshal error path:%s val:%s", preFixName, string(val))
			return
		}
		selector.addrMutex.Lock()
		defer selector.addrMutex.Unlock()
		for i := 0; i < len(selector.addrList); i++ {
			if selector.addrList[i].Handle == addr.Handle {
				selector.addrList[i] = selector.addrList[len(selector.addrList)-1]
				selector.addrList = selector.addrList[:len(selector.addrList)-1]
				i--
			}
		}
	}

	for _, v := range rsp.Kvs {
		addAddr(v.Value)
	}

	go func() {
		watcher := system.etcdClient.Watch(ctx, preFixName, etcd.WithPrefix())
		for r := range watcher {
			for _, event := range r.Events {
				if event.Type == etcd.EventTypePut {
					addAddr(event.Kv.Value)
				} else if event.Type == etcd.EventTypeDelete {
					delAddr(event.Kv.Value)
				}
			}
		}
	}()

	return selector, nil
}

type etcdSelector struct {
	addrMutex sync.Mutex
	addrList  []*ActorAddr
	cancel    context.CancelFunc
}

func (selector *etcdSelector) Addr() *ActorAddr {
	selector.addrMutex.Lock()
	defer selector.addrMutex.Unlock()
	if len(selector.addrList) <= 0 {
		return nil
	}
	return selector.addrList[rand.Int()%len(selector.addrList)]
}

func (selector *etcdSelector) Close() {
	selector.cancel()
	selector.addrList = nil
}

type loopSelector struct {
	etcd  *etcdSelector
	index int
}

func (selector *loopSelector) Addr() *ActorAddr {
	selector.etcd.addrMutex.Lock()
	defer selector.etcd.addrMutex.Unlock()
	if len(selector.etcd.addrList) <= 0 {
		return nil
	}
	index := selector.index
	selector.index++
	return selector.etcd.addrList[index%(len(selector.etcd.addrList))]
}

func (selector *loopSelector) Close() {
	selector.etcd.Close()
}
