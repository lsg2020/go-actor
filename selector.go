package goactor

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"

	etcd "github.com/coreos/etcd/clientv3"
	"go.uber.org/zap"
)

// Selector 根据名字获取Actor地址
type Selector interface {
	Addr() *ActorAddr
	Addrs() []*ActorAddr
	Close()
}

// NewRandomSelector 在同名的actor中随机获取
func NewRandomSelector(system *ActorSystem, name string) (Selector, error) {
	selector, err := newEtcdSelector(system, name)
	if err != nil {
		return nil, err
	}

	return selector, nil
}

// NewLoopSelector 在同名的actor中顺序获取
func NewLoopSelector(system *ActorSystem, name string) (Selector, error) {
	selector, err := newEtcdSelector(system, name)
	if err != nil {
		return nil, err
	}

	return &loopSelector{etcd: selector}, nil
}

// 查询并监听etcd的actor名字注册
func newEtcdSelector(system *ActorSystem, name string) (*etcdSelector, error) {
	preFixName := fmt.Sprintf("/%s/%s/names/%s/", system.options.etcdPrefix, system.options.name, name)

	ctx, cancel := context.WithCancel(system.Context())
	selector := &etcdSelector{
		cancel: cancel,
	}

	addAddr := func(val []byte) {
		addr := &ActorAddr{}
		err := json.Unmarshal(val, addr)
		if err != nil {
			system.Logger().Error("select unmarshal error", zap.String("path", preFixName), zap.String("value", string(val)))
			return
		}
		selector.addrList = append(selector.addrList, addr)
	}

	delAddr := func(instanceId uint64, handle ActorHandle) {
		list := make([]*ActorAddr, 0, len(selector.addrList))
		for i := 0; i < len(selector.addrList); i++ {
			if selector.addrList[i].NodeInstanceId == instanceId && selector.addrList[i].Handle == handle {
				continue
			}
			list = append(list, selector.addrList[i])
		}

		selector.addrList = list
	}

	go func() {
		watcher := system.etcdClient.Watch(ctx, preFixName, etcd.WithPrefix())
		compileRegex := regexp.MustCompile(`/(\w*)/(\w*)`)

		rsp, _ := system.etcdClient.Get(system.Context(), preFixName, etcd.WithPrefix())
		for _, v := range rsp.Kvs {
			addAddr(v.Value)
		}

		for r := range watcher {
			for _, event := range r.Events {
				if event.Type == etcd.EventTypePut {
					addAddr(event.Kv.Value)
				} else if event.Type == etcd.EventTypeDelete {
					path := string(event.Kv.Key)[len(preFixName):]
					paths := compileRegex.FindStringSubmatch(path)
					if len(paths) == 3 {
						instanceId, _ := strconv.ParseUint(paths[1], 10, 64)
						handle, _ := strconv.ParseUint(paths[2], 10, 64)
						delAddr(instanceId, ActorHandle(handle))
					}
				}
			}
		}
	}()

	return selector, nil
}

type etcdSelector struct {
	addrList []*ActorAddr
	cancel   context.CancelFunc
}

func (selector *etcdSelector) Addr() *ActorAddr {
	if len(selector.addrList) <= 0 {
		return nil
	}
	return selector.addrList[rand.Int()%len(selector.addrList)]
}

func (selector *etcdSelector) Addrs() []*ActorAddr {
	return selector.addrList
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
	if len(selector.etcd.addrList) <= 0 {
		return nil
	}
	index := selector.index
	selector.index++
	return selector.etcd.addrList[index%(len(selector.etcd.addrList))]
}

func (selector *loopSelector) Addrs() []*ActorAddr {
	return selector.Addrs()
}

func (selector *loopSelector) Close() {
	selector.etcd.Close()
}
