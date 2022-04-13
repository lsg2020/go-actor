package go_actor

import (
	"context"
	"sync"
	"time"
)

// ActorInstance actor实例需要实现的接口,均在actor绑定的执行器中被执行
type ActorInstance interface {
	OnInit(actor Actor)
	OnRelease(actor Actor)
}

// ActorHandle actor在节点下的地址,节点内唯一
type ActorHandle uint32

// ActorAddr actor在每个ActorSystem中的唯一地址
type ActorAddr struct {
	NodeInstanceId uint64
	Handle         ActorHandle
}

type CallOptions struct {
	Headers []Header
}

// Callback actor消息回调函数
type Callback func(msg *DispatchMessage)

// Actor actor操作接口
type Actor interface {
	Instance() ActorInstance
	Callback() Callback
	Logger() Logger

	// Dispatch 接收到消息准备分发
	Dispatch(system *ActorSystem, msg *DispatchMessage)
	// SendTo 发送消息接口, destination需要注册在同一ActorSystem下的任意节点
	SendTo(system *ActorSystem, destination *ActorAddr, proto Proto, requestCtx interface{}, session int, data interface{}, options *CallOptions) (SessionCancel, *ActorError)
	// SendProto 根据协议id发送不需要返回的消息
	SendProto(system *ActorSystem, destination *ActorAddr, protocol int, options *CallOptions, data ...interface{}) *ActorError
	// CallProto 根据协议id发送需要同步等待返回的消息
	CallProto(ctx context.Context, system *ActorSystem, destination *ActorAddr, protocol int, options *CallOptions, data ...interface{}) ([]interface{}, *ActorError)

	Kill()
	Sleep(d time.Duration)
	Timeout(d time.Duration, cb func())
	Exec(f interface{}, args ...interface{})
}

// actor接口的实现
type actorImpl struct {
	instance ActorInstance
	executer Executer

	actorMutex sync.Mutex
	addrs      map[*ActorSystem]*ActorAddr

	cb          Callback
	protoSystem Proto

	ops *actorOptions
}

func (a *actorImpl) Instance() ActorInstance {
	return a.instance
}

func (a *actorImpl) Logger() Logger {
	return a.ops.logger
}

func (a *actorImpl) setCallback(cb Callback) {
	a.cb = cb
}

func (a *actorImpl) Callback() Callback {
	return a.cb
}

func (a *actorImpl) SendTo(system *ActorSystem, destination *ActorAddr, proto Proto, requestCtx interface{}, session int, data interface{}, options *CallOptions) (SessionCancel, *ActorError) {
	var header Headers
	header = header.Put(
		BuildHeaderInt(HeaderIdProtocol, proto.Id()),
	)
	if destination != nil {
		header = header.Put(BuildHeaderActor(HeaderIdDestination, destination))
	}
	if session != 0 {
		header = header.Put(BuildHeaderInt(HeaderIdSession, session))
	}
	if requestCtx != nil {
		header = header.Put(BuildHeaderInterfaceRaw(HeaderIdRequestProtoPackCtx, requestCtx, true))
	}
	if options != nil && options.Headers != nil {
		header = header.Put(options.Headers...)
	}

	response := a.response
	if session == 0 {
		response = nil
	}

	msg := &DispatchMessage{
		Headers: HeadersWrap{header},
		Content: data,

		RequestProto:     proto,
		DispatchResponse: response,
	}
	interceptor := proto.InterceptorSend()
	if interceptor != nil {
		interceptor(msg, nil)
	}

	var cancel SessionCancel
	if session != 0 {
		cancel = a.executer.StartWait(session, nil)
	}

	cancelTrans, err := system.transport(destination, msg)
	if err != nil {
		return nil, err
	}
	if cancel != nil && cancelTrans != nil {
		return func() {
			cancel()
			cancelTrans()
		}, nil
	}
	if cancel != nil {
		return cancel, nil
	}
	return cancelTrans, nil
}

func (a *actorImpl) getProto(id int) Proto {
	for _, p := range a.ops.protos {
		if p.Id() == id {
			return p
		}
	}
	return nil
}

func (a *actorImpl) SendProto(system *ActorSystem, destination *ActorAddr, protocol int, options *CallOptions, data ...interface{}) *ActorError {
	proto := a.getProto(protocol)
	if proto == nil {
		return ErrProtocolNotExists
	}

	msgs, requestCtx, err := proto.Pack(nil, data...)
	if err != nil {
		return err
	}
	_, err = a.SendTo(system, destination, proto, requestCtx, 0, msgs, options)
	return err
}

func (a *actorImpl) CallProto(ctx context.Context, system *ActorSystem, destination *ActorAddr, protocol int, options *CallOptions, data ...interface{}) ([]interface{}, *ActorError) {
	proto := a.getProto(protocol)
	if proto == nil {
		return nil, ErrProtocolNotExists
	}

	msgs, requestCtx, err := proto.Pack(nil, data...)
	if err != nil {
		return nil, err
	}
	session := a.executer.NewSession()
	cancel, err := a.SendTo(system, destination, proto, requestCtx, session, msgs, options)
	if err != nil {
		return nil, err
	}
	if cancel != nil {
		defer cancel()
	}

	rets, err := a.executer.Wait(ctx, session)
	if err != nil {
		return nil, err
	}

	datas, _, err := proto.UnPack(requestCtx, rets)
	return datas, err
}

func (a *actorImpl) response(msg *DispatchMessage, err *ActorError, data interface{}) {
	a.executer.OnResponse(msg.Headers.GetInt(HeaderIdSession), err, data)
}

func (a *actorImpl) Dispatch(system *ActorSystem, msg *DispatchMessage) {
	msg.System = system
	msg.Actor = a

	a.executer.OnMessage(msg)
}

func (a *actorImpl) onMessage(msg *DispatchMessage) {
	protocol := msg.Headers.GetInt(HeaderIdProtocol)
	p := a.getProto(protocol)
	if p == nil {
		a.Logger().Errorf("actor message protocol not exists %d", protocol)
		return
	}
	msg.RequestProto = p

	p.OnMessage(msg)
}

func (a *actorImpl) onRegister(system *ActorSystem, addr *ActorAddr) {
	a.actorMutex.Lock()
	a.addrs[system] = addr
	a.actorMutex.Unlock()
}

func (a *actorImpl) onUnregister(system *ActorSystem) {
	a.actorMutex.Lock()
	delete(a.addrs, system)
	a.actorMutex.Unlock()
}

func (a *actorImpl) onKill() {
	defer func() {
		a.actorMutex.Lock()
		systems := make([]*ActorSystem, 0, len(a.addrs))
		for k := range a.addrs {
			systems = append(systems, k)
		}
		a.actorMutex.Unlock()

		for _, system := range systems {
			system.UnRegister(a)
		}
	}()

	a.Instance().OnRelease(a)
}

func (a *actorImpl) dispatchSystemProto(args ...interface{}) {
	data, _, _ := a.protoSystem.Pack(nil, args...)
	var headers Headers
	headers = headers.Put(
		BuildHeaderInt(HeaderIdProtocol, a.protoSystem.Id()),
	)

	msg := &DispatchMessage{
		Headers: HeadersWrap{headers},
		Content: data,
	}
	interceptor := a.protoSystem.InterceptorSend()
	if interceptor != nil {
		interceptor(msg, nil)
	}
	a.Dispatch(nil, msg)
}

func (a *actorImpl) Kill() {
	a.dispatchSystemProto("kill")
}

func (a *actorImpl) Exec(f interface{}, args ...interface{}) {
	a.dispatchSystemProto("exec", f, args)
}

func (a *actorImpl) Timeout(d time.Duration, cb func()) {
	session := a.executer.NewSession()
	a.executer.StartWait(session, func(msg *DispatchMessage) {
		cb()
	})

	time.AfterFunc(d, func() {
		a.executer.OnResponse(session, nil, nil)
	})
}

func (a *actorImpl) Sleep(d time.Duration) {
	session := a.executer.NewSession()
	cancel := a.executer.StartWait(session, nil)
	if cancel != nil {
		defer cancel()
	}
	time.AfterFunc(d, func() {
		a.executer.OnResponse(session, nil, nil)
	})

	_, _ = a.executer.Wait(context.Background(), session)
}

type actorOptions struct {
	logger Logger
	protos []Proto
	initcb func()
}

func (ops *actorOptions) init() {
	if ops.logger == nil {
		ops.logger = DefaultLogger()
	}

}

// ActorOption actor的创建参数
type ActorOption func(ops *actorOptions)

func ActorWithLogger(logger Logger) ActorOption {
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

// NewActor 创建一个actor,立即返回,如果需要等待创建完成可以使用 ActorWithInitCB
func NewActor(inst ActorInstance, executer Executer, options ...ActorOption) Actor {
	ops := &actorOptions{}
	for _, opt := range options {
		opt(ops)
	}
	ops.init()

	a := &actorImpl{
		instance: inst,
		executer: executer,
		addrs:    make(map[*ActorSystem]*ActorAddr),
		ops:      ops,
	}

	a.protoSystem = NewProtoSystem(ops.logger)
	a.ops.protos = append(a.ops.protos, a.protoSystem)

	a.setCallback(a.onMessage)

	// init message
	a.dispatchSystemProto("init")
	return a
}
