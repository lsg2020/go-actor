package goactor

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
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
type ExecCallback func() (interface{}, error)
type ForkCallback func()

type ActorState int

const (
	ActorStateInit    ActorState = 1
	ActorStateRunning ActorState = 2
	ActorStateClosing ActorState = 3
	ActorStateStop    ActorState = 4
)

// Actor actor操作接口
type Actor interface {
	Instance() ActorInstance
	Callback() Callback
	Logger() *zap.Logger
	SetLogger(logger *zap.Logger)
	GetExecuter() Executer
	Context() context.Context
	GetState() ActorState
	GetProto(id int) Proto
	GetAddr(system *ActorSystem) *ActorAddr

	Kill()
	Sleep(d time.Duration)
	Timeout(d time.Duration, cb func())
	Exec(ctx context.Context, f ExecCallback) (interface{}, error)
	Fork(f ForkCallback)

	GenSession() int
	Wait(ctx context.Context, session int) (interface{}, error)
	Wakeup(session int, err error, data interface{})

	// Dispatch 接收到消息准备分发
	Dispatch(system *ActorSystem, msg *DispatchMessage)
	// SendProto 根据协议id发送不需要返回的消息
	SendProto(system *ActorSystem, destination *ActorAddr, protocol int, options *CallOptions, data ...interface{}) error
	// CallProto 根据协议id发送需要同步等待返回的消息
	CallProto(ctx context.Context, system *ActorSystem, destination *ActorAddr, protocol int, options *CallOptions, data ...interface{}) ([]interface{}, error)
}

// actor接口的实现
type actorImpl struct {
	instance ActorInstance
	executer Executer
	context  context.Context
	cancel   context.CancelFunc
	state    ActorState

	actorMutex   sync.Mutex
	addrs        map[*ActorSystem]*ActorAddr
	waitSessions map[int]struct{}

	protoAdmin Proto
	cb         Callback
	ops        *actorOptions
}

func (a *actorImpl) Instance() ActorInstance {
	return a.instance
}

func (a *actorImpl) SetLogger(logger *zap.Logger) {
	a.ops.logger = logger
}

func (a *actorImpl) Logger() *zap.Logger {
	return a.ops.logger
}

func (a *actorImpl) Callback() Callback {
	return a.cb
}

func (a *actorImpl) GetExecuter() Executer {
	return a.executer
}

func (a *actorImpl) GetState() ActorState {
	return a.state
}

func (a *actorImpl) response(msg *DispatchMessage, err error, data interface{}) {
	a.executer.OnResponse(msg.Session(), err, data)
}

func (a *actorImpl) buildSendPack(destination *ActorAddr, proto Proto, protocolCtx interface{}, session int, data interface{}, options *CallOptions) *DispatchMessage {
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
	if options != nil && options.Headers != nil {
		header = header.Put(options.Headers...)
	}

	response := a.response
	if session == 0 {
		response = nil
	}

	msg := NewDispatchMessage(destination, proto, protocolCtx, proto.Id(), session, header, data, nil, response)
	return msg
}

func (a *actorImpl) GetProto(id int) Proto {
	for _, p := range a.ops.protos {
		if p.Id() == id {
			return p
		}
	}
	return nil
}

func (a *actorImpl) preWait(session int, cb func(msg *DispatchMessage)) SessionCancel {
	asyncCB := cb
	if cb != nil {
		asyncCB = func(msg *DispatchMessage) {
			defer func() {
				if r := recover(); r != nil {
					if a.GetState() != ActorStateStop {
						a.Logger().Error("actor message error", zap.Any("recover", r))
					}
				}
			}()
			cb(msg)
		}
	}
	cancel := a.executer.PreWait(session, asyncCB)
	a.actorMutex.Lock()
	a.waitSessions[session] = struct{}{}
	a.actorMutex.Unlock()

	return func() {
		a.actorMutex.Lock()
		delete(a.waitSessions, session)
		a.actorMutex.Unlock()
		if cancel != nil {
			cancel()
		}
	}
}

func (a *actorImpl) wait(ctx context.Context, session int) (interface{}, error) {
	if a.GetState() == ActorStateStop {
		panic("actor stop")
	}

	r, err := a.executer.Wait(ctx, session)
	if a.GetState() == ActorStateStop {
		panic("actor stop")
	}

	return r, err
}

func (a *actorImpl) SendProto(system *ActorSystem, destination *ActorAddr, protocol int, options *CallOptions, data ...interface{}) error {
	proto := a.GetProto(protocol)
	if proto == nil {
		return ErrProtocolNotExists
	}

	msg, protocolCtx, err := proto.PackRequest(data...)
	if err != nil {
		return err
	}

	sendPack := a.buildSendPack(destination, proto, protocolCtx, 0, msg, options)
	sendHandler := func(msg *DispatchMessage, args ...interface{}) error {
		cancelTrans, err := system.transport(destination, msg)
		if err != nil {
			return err
		}
		if cancelTrans != nil {
			defer cancelTrans()
		}
		return nil
	}

	interceptor := proto.InterceptorCall()
	if interceptor != nil {
		return interceptor(sendPack, sendHandler)
	}
	return sendHandler(sendPack)
}

func (a *actorImpl) CallProto(ctx context.Context, system *ActorSystem, destination *ActorAddr, protocol int, options *CallOptions, data ...interface{}) ([]interface{}, error) {
	proto := a.GetProto(protocol)
	if proto == nil {
		return nil, ErrProtocolNotExists
	}

	msg, protocolCtx, err := proto.PackRequest(data...)
	if err != nil {
		return nil, err
	}
	session := a.executer.NewSession()

	var rets interface{}
	sendPack := a.buildSendPack(destination, proto, protocolCtx, session, msg, options)
	callHandler := func(msg *DispatchMessage, args ...interface{}) error {
		cancelSession := a.preWait(session, nil)
		defer cancelSession()

		cancelTrans, err := system.transport(destination, msg)
		if err != nil {
			return err
		}
		if cancelTrans != nil {
			defer cancelTrans()
		}
		rets, err = a.wait(ctx, session)
		if err != nil {
			return err
		}
		return nil
	}

	interceptor := proto.InterceptorCall()
	if interceptor != nil {
		err = interceptor(sendPack, callHandler)
	} else {
		err = callHandler(sendPack)
	}
	if err != nil {
		return nil, err
	}

	datas, err := proto.UnPackResponse(protocolCtx, rets)
	return datas, err
}

func (a *actorImpl) Dispatch(system *ActorSystem, msg *DispatchMessage) {
	msg.System = system
	msg.Actor = a

	a.executer.OnMessage(msg)
}

func (a *actorImpl) GetAddr(system *ActorSystem) *ActorAddr {
	a.actorMutex.Lock()
	addr := a.addrs[system]
	a.actorMutex.Unlock()
	return addr
}

func (a *actorImpl) onRegisterSystem(system *ActorSystem, addr *ActorAddr) {
	a.actorMutex.Lock()
	a.addrs[system] = addr
	a.actorMutex.Unlock()
}

func (a *actorImpl) onUnregisterSystem(system *ActorSystem) {
	a.actorMutex.Lock()
	delete(a.addrs, system)
	a.actorMutex.Unlock()
}

func (a *actorImpl) onInit() {
	defer func() {
		if r := recover(); r != nil {
			a.Logger().Error("actor start error", zap.Any("recover", r))
			a.Kill()
		}
	}()

	a.Instance().OnInit(a)

	if a.ops.initcb != nil {
		a.ops.initcb()
	}
	a.state = ActorStateRunning
}

func (a *actorImpl) onKill() {
	if a.state != ActorStateRunning {
		return
	}
	a.state = ActorStateClosing

	defer func() {
		a.state = ActorStateStop
		a.cancel()
		a.cb = nil

		a.actorMutex.Lock()
		systems := make([]*ActorSystem, 0, len(a.addrs))
		sessions := make([]int, 0, len(a.waitSessions))
		for k := range a.addrs {
			systems = append(systems, k)
		}
		for k := range a.waitSessions {
			sessions = append(sessions, k)
		}
		a.actorMutex.Unlock()

		for _, session := range sessions {
			a.executer.OnResponse(session, nil, nil)
		}
		for _, system := range systems {
			system.UnRegister(a)
		}
	}()

	a.Instance().OnRelease(a)
}

func (a *actorImpl) dispatchAdminProto(session int, args ...interface{}) {
	data, _, _ := a.protoAdmin.PackRequest(args...)
	var headers Headers
	headers = headers.Put(
		BuildHeaderInt(HeaderIdProtocol, a.protoAdmin.Id()),
	)
	response := a.response
	if session != 0 {
		headers = headers.Put(BuildHeaderInt(HeaderIdSession, session))
	} else {
		response = nil
	}

	msg := NewDispatchMessage(nil, a.protoAdmin, nil, a.protoAdmin.Id(), session, headers, data, nil, response)
	systemHandler := func(msg *DispatchMessage, args ...interface{}) error {
		a.Dispatch(nil, msg)
		return nil
	}

	interceptor := a.protoAdmin.InterceptorCall()
	if interceptor != nil {
		_ = interceptor(msg, systemHandler)
	} else {
		_ = systemHandler(msg)
	}
}

func (a *actorImpl) Kill() {
	a.dispatchAdminProto(0, "kill")
}

func (a *actorImpl) Exec(ctx context.Context, f ExecCallback) (interface{}, error) {
	session := a.executer.NewSession()
	cancel := a.preWait(session, nil)
	defer cancel()

	a.dispatchAdminProto(session, "exec", f)
	r, err := a.wait(ctx, session)
	return r, err
}

func (a *actorImpl) Fork(f ForkCallback) {
	a.dispatchAdminProto(0, "fork", f)
}

func (a *actorImpl) Timeout(d time.Duration, cb func()) {
	session := a.executer.NewSession()
	var cancel SessionCancel
	cancel = a.preWait(session, func(msg *DispatchMessage) {
		cancel()
		cb()
	})

	time.AfterFunc(d, func() {
		a.executer.OnResponse(session, nil, nil)
	})
}

func (a *actorImpl) Sleep(d time.Duration) {
	session := a.executer.NewSession()
	cancel := a.preWait(session, nil)
	defer cancel()

	time.AfterFunc(d, func() {
		a.executer.OnResponse(session, nil, nil)
	})

	_, _ = a.wait(a.Context(), session)
}

func (a *actorImpl) GenSession() int {
	return a.executer.NewSession()
}

func (a *actorImpl) Wait(ctx context.Context, session int) (interface{}, error) {
	cancel := a.preWait(session, nil)
	defer cancel()
	return a.wait(ctx, session)
}

func (a *actorImpl) Wakeup(session int, err error, data interface{}) {
	a.executer.OnResponse(session, err, data)
}

func (a *actorImpl) Context() context.Context {
	return a.context
}

// NewActor 创建一个actor,立即返回,如果需要等待创建完成可以使用 ActorWithInitCB
func NewActor(inst ActorInstance, executer Executer, options ...ActorOption) Actor {
	ops := &actorOptions{}
	for _, opt := range options {
		opt(ops)
	}
	ops.init()

	a := &actorImpl{
		ops:          ops,
		instance:     inst,
		executer:     executer,
		state:        ActorStateInit,
		addrs:        make(map[*ActorSystem]*ActorAddr),
		waitSessions: make(map[int]struct{}),
	}

	a.cb = func(msg *DispatchMessage) {
		defer func() {
			if r := recover(); r != nil {
				if a.GetState() != ActorStateStop {
					msg.MaybeResponseErr(ErrMessageErr)
					a.Logger().Error("actor message error", zap.Any("recover", r))
				}
			}
			//if msg.DispatchResponse != nil && msg.Headers.GetInt(HeaderIdSession) != 0 {
			//	a.Logger().Error("maybe forget response")
			//	msg.Response(ErrForgetResponse, nil)
			//}
		}()

		protocol := msg.Headers.GetInt(HeaderIdProtocol)
		p := a.GetProto(protocol)
		if p == nil {
			msg.MaybeResponseErr(ErrProtocolNotExists)
			a.Logger().Error("actor message protocol not exists", zap.Int("protocol", protocol))
			return
		}
		msg.Protocol = p

		p.OnMessage(msg)
	}

	a.context, a.cancel = context.WithCancel(ops.ctx)

	a.protoAdmin = a.GetProto(ProtocolAdmin)
	if a.protoAdmin == nil {
		p := &ProtoAdmin{
			ProtoBaseImpl: ProtoBaseBuild(
				ProtoWithInterceptorCall(func(msg *DispatchMessage, handler ProtoHandler, args ...interface{}) error {
					msg.Headers.Put(BuildHeaderString(HeaderIdMethod, msg.Content.([]interface{})[0].(string)))
					return handler(msg, args...)
				}),
				ProtoWithInterceptorDispatch(func(msg *DispatchMessage, handler ProtoHandler, args ...interface{}) error {
					defer func() {
						if msg.Actor != nil && msg.Actor.GetState() == ActorStateStop {
							return
						}
						if r := recover(); r != nil {
							logger := DefaultLogger()
							if msg.Actor != nil {
								logger = msg.Actor.Logger()
							}
							logger.Error("system recover", zap.String("cmd", msg.Headers.GetStr(HeaderIdMethod)))
							panic(r)
						}
					}()
					return handler(msg, args...)
				}),
			),
		}
		p.Init()
		a.protoAdmin = p
		a.ops.protos = append(a.ops.protos, a.protoAdmin)
	}

	// init message
	a.dispatchAdminProto(0, "init")
	return a
}
