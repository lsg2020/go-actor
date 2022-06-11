package goactor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// ActorSystem actor的管理器,及提供对外访问同system下的actor的机制
type ActorSystem struct {
	instanceID uint64              // ActorSystem实例id,需要全局唯一
	options    *actorSystemOptions // ActorSystem的创建参数
	handle     systemHandle        // 本地actor的管理
	context    context.Context
	cancel     context.CancelFunc

	etcdClient  *etcd.Client // 同一ActorSystem需要连接相同的etcd
	etcdSession *concurrency.Session

	namesMutex sync.Mutex
	names      map[ActorHandle][]string // 本地actor注册的名字信息

	nodeMutex sync.Mutex
	nodes     []*ActorNodeConfig // 同ActorSystem的其他节点地址信息
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

	system.Logger().Info("actor system start")
	return system, nil
}

func (system *ActorSystem) init() error {
	var err error
	system.options, err = system.options.init()
	if err != nil {
		return err
	}
	system.instanceID = system.options.instanceID
	system.options.logger = system.options.logger.With(zap.Uint64("actor_system_node_id", system.instanceID), zap.String("actor_system_name", system.options.name))
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

// NodeConfig 根据instanceId获取同一ActorSystem的其他节点信息
func (system *ActorSystem) NodeConfig(instanceId uint64) *ActorNodeConfig {
	system.nodeMutex.Lock()
	defer system.nodeMutex.Unlock()

	for _, node := range system.nodes {
		if node.InstanceID == instanceId {
			return node
		}
	}

	return nil
}

// 注册并监听其他节点地址信息
func (system *ActorSystem) initEtcd() error {
	if system.options.etcdClient == nil {
		etcdClient, err := etcd.New(etcd.Config{
			Endpoints:   system.options.etcd,
			DialTimeout: time.Second * 5,
		})
		if err != nil {
			return errors.Wrapf(err, "etcds:%v", system.options.etcd)
		}
		system.etcdClient = etcdClient
	} else {
		system.etcdClient = system.options.etcdClient
	}

	var err error
	etcdClient := system.etcdClient
	ctx, cancel := context.WithCancel(etcdClient.Ctx())
	connected := false
	time.AfterFunc(time.Second*5, func() {
		if !connected {
			cancel()
		}
	})
	system.etcdSession, err = concurrency.NewSession(etcdClient, concurrency.WithTTL(15), concurrency.WithContext(ctx))
	if err != nil {
		return errors.Wrapf(err, "etcd connect failed:%v", system.options.etcd)
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
			return errors.Wrapf(err, "register etcd")
		}

		key := fmt.Sprintf("/%s/%s/nodes/%d", system.options.etcdPrefix, system.options.name, system.instanceID)
		_, err = etcdClient.Put(system.Context(), key, string(v), etcd.WithLease(system.etcdSession.Lease()))
		if err != nil {
			return errors.Wrapf(err, "register etcd key:%s", key)
		}
	}

	addnode := func(k, v []byte) {
		system.nodeMutex.Lock()
		defer system.nodeMutex.Unlock()

		config := &ActorNodeConfig{}
		err := json.Unmarshal(v, config)
		if err != nil {
			return
		}

		system.nodes = append(system.nodes, config)
	}

	delnode := func(nodeInstanceId uint64) {
		system.nodeMutex.Lock()
		defer system.nodeMutex.Unlock()

		for index, node := range system.nodes {
			if node.InstanceID == nodeInstanceId {
				system.nodes[index] = system.nodes[len(system.nodes)-1]
				system.nodes = system.nodes[:len(system.nodes)-1]
				break
			}
		}
	}

	go func() {
		key := fmt.Sprintf("/%s/%s/nodes", system.options.etcdPrefix, system.options.name)

		watcher := etcdClient.Watch(system.Context(), key, etcd.WithPrefix())
		resp, _ := etcdClient.Get(system.Context(), key, etcd.WithPrefix())
		for _, kv := range resp.Kvs {
			addnode(kv.Key, kv.Value)
		}

		for rsp := range watcher {
			for _, ev := range rsp.Events {
				if ev.Type == etcd.EventTypePut {
					addnode(ev.Kv.Key, ev.Kv.Value)
				}
				if ev.Type == etcd.EventTypeDelete {
					instanceStr := string(ev.Kv.Key)[len(key)+1:]
					instanceId, err := strconv.ParseUint(instanceStr, 10, 64)
					if err == nil {
						delnode(instanceId)
					}
				}
			}
		}
	}()

	return nil
}

func (system *ActorSystem) InstanceID() uint64 {
	return system.instanceID
}

// Register 注册actor到ActorSystem上,只有在ActorSystem上的Actor才可以被外部访问
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
			a.Logger().Error("register actor name error", zap.String("system_name", system.options.name), zap.String("actor_name", name), zap.Error(err))
		}
	}
	system.Logger().Info("actor register", zap.Uint32("actor_handle", uint32(addr.Handle)))
	return addr
}

func (system *ActorSystem) UnRegister(a Actor) {
	ret, handle := system.handle.handleRetire(a)
	if a, ok := a.(*actorImpl); ok {
		a.onUnregister(system)
	}

	system.namesMutex.Lock()
	names := system.names[handle]
	system.namesMutex.Unlock()
	for _, name := range names {
		system.UnbindName(name, handle)
	}

	system.Logger().Info("actor unregister", zap.Uint32("actor_handle", uint32(handle)), zap.Bool("ret", ret))
}

// BindName 给Actor一个服务名
func (system *ActorSystem) BindName(name string, addr *ActorAddr) error {
	key := fmt.Sprintf("/%s/%s/names/%s/%d/%d", system.options.etcdPrefix, system.options.name, name, addr.NodeInstanceId, addr.Handle)
	v, err := json.Marshal(addr)
	if err != nil {
		return err
	}
	_, err = system.etcdClient.Put(system.Context(), key, string(v), etcd.WithLease(system.etcdSession.Lease()))
	if err != nil {
		return err
	}

	system.namesMutex.Lock()
	defer system.namesMutex.Unlock()
	system.names[addr.Handle] = append(system.names[addr.Handle], name)
	return nil
}

func (system *ActorSystem) UnbindName(name string, handle ActorHandle) {
	system.namesMutex.Lock()
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
	system.namesMutex.Unlock()

	key := fmt.Sprintf("/%s/%s/names/%s/%d/%d", system.options.etcdPrefix, system.options.name, name, system.options.instanceID, handle)
	_, _ = system.etcdClient.Delete(system.Context(), key)
}

func (system *ActorSystem) IsRemoteActor(addr *ActorAddr) bool {
	if addr == nil {
		return true
	}
	return system.instanceID != addr.NodeInstanceId
}

// Dispatch 分发ActorSystem本节点的Actor消息
func (system *ActorSystem) Dispatch(msg *DispatchMessage) error {
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

// 传输一个消息,本节点直接分发,其他节点可以指定HeaderIdTransport传输类型,默认使用第一个
func (system *ActorSystem) transport(destination *ActorAddr, msg *DispatchMessage) (SessionCancel, error) {
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

func (system *ActorSystem) Logger() *zap.Logger {
	return system.options.logger
}

func (system *ActorSystem) Context() context.Context {
	return system.context
}

func (system *ActorSystem) Close() {
	system.cancel()
}
