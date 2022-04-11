package gactor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type ActorSystem struct {
	instanceID uint64
	options    *actorSystemOptions
	handle     systemHandle
	context    context.Context
	cancel     context.CancelFunc

	etcdClient  *etcd.Client
	etcdSession *concurrency.Session

	namesMux sync.Mutex
	names    map[ActorHandle][]string

	nodeMux sync.Mutex
	nodes   []*ActorNodeConfig
}

func NewActorSystem(opts ...ActorSystemOption) (*ActorSystem, error) {
	options := &actorSystemOptions{}
	for _, opt := range opts {
		opt(options)
	}
	system := &ActorSystem{
		options: options,
		handle:  systemHandle{},
		names:   make(map[ActorHandle][]string),
	}

	err := system.init()
	if err != nil {
		return nil, err
	}

	system.Logger().Infof("actor system start %s:%d\n", system.options.name, system.instanceID)
	return system, nil
}

func (system *ActorSystem) init() error {
	var err error
	system.options, err = system.options.init()
	if err != nil {
		return err
	}
	system.instanceID = system.options.instanceID
	system.handle.handleInit()

	system.context, system.cancel = context.WithCancel(system.options.ctx)

	// init transport
	for _, transport := range system.options.transports {
		err := transport.Init(system)
		if err != nil {
			return err
		}
	}

	// init etcd
	err = system.initEtcd()
	if err != nil {
		return err
	}

	return nil
}

type ActorNodeConfig struct {
	InstanceID uint64
	Transports map[string]string
}

func (system *ActorSystem) NodeConfig(instanceId uint64) *ActorNodeConfig {
	system.nodeMux.Lock()
	defer system.nodeMux.Unlock()

	for _, node := range system.nodes {
		if node.InstanceID == instanceId {
			return node
		}
	}

	return nil
}

func (system *ActorSystem) initEtcd() error {
	etcdClient, err := etcd.New(etcd.Config{
		Endpoints:   system.options.etcd,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		return ErrorWrap(err)
	}
	system.etcdClient = etcdClient

	ctx, cancel := context.WithCancel(etcdClient.Ctx())
	connected := false
	time.AfterFunc(time.Second*5, func() {
		if !connected {
			cancel()
		}
	})
	system.etcdSession, err = concurrency.NewSession(etcdClient, concurrency.WithTTL(15), concurrency.WithContext(ctx))
	if err != nil {
		return Errorf("etcd connect failed %s", system.options.etcd)
	}
	connected = true

	{
		// register etcd
		transports := make(map[string]string)
		for _, trans := range system.options.transports {
			transports[trans.Name()] = trans.URI()
		}
		v, err := json.Marshal(&ActorNodeConfig{
			system.instanceID,
			transports,
		})
		if err != nil {
			return ErrorWrap(err)
		}

		key := fmt.Sprintf("/%s/%s/nodes/%d", system.options.etcdPrefix, system.options.name, system.instanceID)
		_, err = etcdClient.Put(system.Context(), key, string(v), etcd.WithLease(system.etcdSession.Lease()))
		if err != nil {
			return ErrorWrap(err)
		}
	}

	addnode := func(k, v []byte) {
		system.nodeMux.Lock()
		defer system.nodeMux.Unlock()

		config := &ActorNodeConfig{}
		err := json.Unmarshal(v, config)
		if err != nil {
			return
		}

		system.nodes = append(system.nodes, config)
	}

	delnode := func(k, v []byte) {
		system.nodeMux.Lock()
		defer system.nodeMux.Unlock()

		config := &ActorNodeConfig{}
		err := json.Unmarshal(v, config)
		if err != nil {
			return
		}

		for index, node := range system.nodes {
			if node.InstanceID == config.InstanceID {
				system.nodes[index] = system.nodes[len(system.nodes)-1]
				system.nodes = system.nodes[:len(system.nodes)-1]
				break
			}
		}
	}

	go func() {
		key := fmt.Sprintf("/%s/%s/nodes", system.options.etcdPrefix, system.options.name)
		resp, _ := etcdClient.Get(system.Context(), key, etcd.WithPrefix())
		for _, kv := range resp.Kvs {
			addnode(kv.Key, kv.Value)
		}

		watcher := etcdClient.Watch(system.Context(), key, etcd.WithPrefix())
		for rsp := range watcher {
			for _, ev := range rsp.Events {
				if ev.Type == etcd.EventTypePut {
					addnode(ev.Kv.Key, ev.Kv.Value)
				}
				if ev.Type == etcd.EventTypeDelete {
					delnode(ev.Kv.Key, ev.Kv.Value)
				}
			}
		}
	}()

	return nil
}

func (system *ActorSystem) InstanceID() uint64 {
	return system.instanceID
}

func (system *ActorSystem) IsServer() bool {
	return system.options.server
}

func (system *ActorSystem) Register(a Actor, names ...string) *ActorAddr {
	handle := system.handle.handleRegister(a)

	addr := &ActorAddr{
		NodeInstanceId: system.instanceID,
		Handle:         handle,
	}

	if a, ok := a.(*actorImpl); ok {
		a.onRegister(system, addr)
	}

	for _, name := range names {
		err := system.BindName(name, addr)
		if err != nil {
			system.Logger().Errorf("register actor name error:%#v", err.Error())
		}
	}
	system.Logger().Infof("%s:%d actor register %#v\n", system.options.name, system.instanceID, addr)
	return addr
}

func (system *ActorSystem) UnRegister(a Actor) {
	ret, handle := system.handle.handleRetire(a)
	if a, ok := a.(*actorImpl); ok {
		a.onUnregister(system)
	}

	system.namesMux.Lock()
	names := system.names[handle]
	system.namesMux.Unlock()
	for _, name := range names {
		system.UnbindName(name, handle)
	}

	system.Logger().Infof("%s:%d actor unregister %v %v\n", system.options.name, system.instanceID, ret, handle)
}

func (system *ActorSystem) BindName(name string, addr *ActorAddr) error {
	key := fmt.Sprintf("/%s/%s/names/%s/%d", system.options.etcdPrefix, system.options.name, name, addr.Handle)
	v, err := json.Marshal(addr)
	if err != nil {
		return err
	}
	_, err = system.etcdClient.Put(system.Context(), key, string(v), etcd.WithLease(system.etcdSession.Lease()))
	if err != nil {
		return err
	}

	system.namesMux.Lock()
	defer system.namesMux.Unlock()
	system.names[addr.Handle] = append(system.names[addr.Handle], name)
	return nil
}

func (system *ActorSystem) UnbindName(name string, handle ActorHandle) {
	system.namesMux.Lock()
	names := system.names[handle]
	for i := 0; i < len(names); i++ {
		if names[i] == name {
			names[i] = names[len(names)-1]
			names = names[:len(names)-1]
			i--
		}
	}
	if len(names) > 0 {
		system.names[handle] = names
	} else {
		delete(system.names, handle)
	}
	system.namesMux.Unlock()

	key := fmt.Sprintf("/%s/%s/names/%s/%d", system.options.etcdPrefix, system.options.name, name, handle)
	_, _ = system.etcdClient.Delete(system.Context(), key)
}

func (system *ActorSystem) IsRemoteActor(addr *ActorAddr) bool {
	if addr == nil {
		return true
	}
	return system.instanceID != addr.NodeInstanceId
}

func (system *ActorSystem) Dispatch(msg *DispatchMessage) *ActorError {
	destination := msg.Headers.GetAddr(HeaderIdDestination)
	if destination == nil {
		return ErrNeedDestination
	}
	destActor := system.handle.find(destination.Handle)
	if destActor == nil {
		return ErrActorMiss
	}

	destActor.Dispatch(system, msg)
	return nil
}

func (system *ActorSystem) transport(destination *ActorAddr, msg *DispatchMessage) (SessionCancel, *ActorError) {
	if system.IsRemoteActor(destination) {
		if len(system.options.transports) == 0 {
			return nil, ErrTransportMiss
		}
		trans := system.options.transports[0]
		transOpt := msg.Headers.GetInterface(HeaderIdTransport)
		if transOpt != nil {
			var ok bool
			trans, ok = transOpt.(Transport)
			if !ok {
				return nil, ErrTransportMiss
			}
		}
		return trans.Send(msg)
	}

	err := system.Dispatch(msg)
	return nil, err
}

func (system *ActorSystem) Logger() Logger {
	return system.options.logger
}

func (system *ActorSystem) Context() context.Context {
	return system.context
}

func (system *ActorSystem) Close() {
	system.cancel()
}
