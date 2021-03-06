package goactor

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	defaultEtcdTTL = 30
)

type ActorSystemOption func(ops *actorSystemOptions)

type actorSystemOptions struct {
	instanceID uint64
	name       string
	etcdClient *etcd.Client
	etcd       []string
	etcdPrefix string
	transports []Transport
	logger     *zap.Logger
	ctx        context.Context
	ttl        int
}

func (options *actorSystemOptions) init() (*actorSystemOptions, error) {
	opts := options
	if opts == nil {
		opts = &actorSystemOptions{}
	}
	if opts.name == "" {
		return opts, ErrInitNeedName
	}
	if opts.instanceID == 0 {
		return opts, ErrInitNeedInstanceId
	}

	if opts.etcd == nil {
		opts.etcd = []string{"http://127.0.0.1:2379"}
	}
	if opts.etcdPrefix == "" {
		opts.etcdPrefix = "gactor"
	}
	if opts.logger == nil {
		opts.logger = DefaultLogger()
	}
	if opts.ctx == nil {
		opts.ctx = context.Background()
	}
	if opts.ttl == 0 {
		opts.ttl = defaultEtcdTTL
	}

	return opts, nil
}

func WithTransport(transport Transport) ActorSystemOption {
	return func(options *actorSystemOptions) {
		options.transports = append(options.transports, transport)
	}
}

func WithEtcdClient(client *etcd.Client) ActorSystemOption {
	return func(options *actorSystemOptions) {
		options.etcdClient = client
	}
}

func WithEtcd(addr ...string) ActorSystemOption {
	return func(options *actorSystemOptions) {
		options.etcd = append(options.etcd, addr...)
	}
}

func WithName(name string) ActorSystemOption {
	return func(options *actorSystemOptions) {
		options.name = name
	}
}

func WithInstanceId(id uint64) ActorSystemOption {
	return func(options *actorSystemOptions) {
		options.instanceID = id
	}
}

func WithLogger(logger *zap.Logger) ActorSystemOption {
	return func(options *actorSystemOptions) {
		options.logger = logger
	}
}

func WithContext(ctx context.Context) ActorSystemOption {
	return func(options *actorSystemOptions) {
		options.ctx = ctx
	}
}

func WithTTL(ttl int) ActorSystemOption {
	return func(optionss *actorSystemOptions) {
		optionss.ttl = ttl
	}
}

type actorOptions struct {
	logger *zap.Logger
	protos []Proto
	initcb func()
	ctx    context.Context
}

func (ops *actorOptions) init() {
	if ops.logger == nil {
		ops.logger = DefaultLogger()
	}
	if ops.ctx == nil {
		ops.ctx = context.Background()
	}

}

// ActorOption actor???????????????
type ActorOption func(ops *actorOptions)

func ActorWithLogger(logger *zap.Logger) ActorOption {
	return func(ops *actorOptions) {
		ops.logger = logger
	}
}

func ActorWithProto(proto Proto) ActorOption {
	return func(ops *actorOptions) {
		ops.protos = append(ops.protos, proto)
	}
}

func ActorWithInitCB(cb func()) ActorOption {
	return func(ops *actorOptions) {
		ops.initcb = cb
	}
}

func ActorWithContext(ctx context.Context) ActorOption {
	return func(ops *actorOptions) {
		ops.ctx = ctx
	}
}
