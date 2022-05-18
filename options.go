package goactor

import "context"

type ActorSystemOption func(ops *actorSystemOptions)

type actorSystemOptions struct {
	instanceID uint64
	name       string
	etcd       []string
	etcdPrefix string
	transports []Transport
	logger     Logger
	ctx        context.Context
}

func (options *actorSystemOptions) init() (*actorSystemOptions, error) {
	if options == nil {
		options = &actorSystemOptions{}
	}
	if options.name == "" {
		return options, Errorf("actor system name not set")
	}
	if options.instanceID == 0 {
		return options, Errorf("actor system instance id not set")
	}

	if options.etcd == nil {
		options.etcd = []string{"http://127.0.0.1:2379"}
	}
	if options.etcdPrefix == "" {
		options.etcdPrefix = "gactor"
	}
	if options.logger == nil {
		options.logger = DefaultLogger()
	}
	if options.ctx == nil {
		options.ctx = context.Background()
	}

	return options, nil
}

func WithTransport(transport Transport) ActorSystemOption {
	return func(options *actorSystemOptions) {
		options.transports = append(options.transports, transport)
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

func WithLogger(logger Logger) ActorSystemOption {
	return func(options *actorSystemOptions) {
		options.logger = logger
	}
}

func WithContext(ctx context.Context) ActorSystemOption {
	return func(options *actorSystemOptions) {
		options.ctx = ctx
	}
}
