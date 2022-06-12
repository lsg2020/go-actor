package goactor

import "go.uber.org/zap"

// NewProtoSystem 创建一个内部协议,处理init/kill/exec
func NewProtoSystem() *ProtoSystem {
	p := &ProtoSystem{
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
	p.init()
	return p
}

type ProtoSystem struct {
	ProtoBaseImpl
	cmds map[string]ProtoHandler
}

func (p *ProtoSystem) Id() int {
	return ProtocolSystem
}

func (p *ProtoSystem) Name() string {
	return "system"
}

func (p *ProtoSystem) GetOptions() []ProtoOption {
	return nil
}

func (p *ProtoSystem) Register(cmd string, cb ProtoHandler, _ ...interface{}) {
	p.cmds[cmd] = cb
}

func (p *ProtoSystem) OnMessage(msg *DispatchMessage) {
	datas, _, _ := p.UnPack(nil, msg.Content)

	cmd := datas[0].(string)
	cb := p.cmds[cmd]
	if cb == nil {
		msg.Actor.Logger().Error("system message not exists", zap.String("cmd", cmd))
		return
	}

	err := p.Trigger(cb, msg, datas[1:]...)
	if err != nil {
		msg.Actor.Logger().Error("system msg error", zap.String("cmd", cmd), zap.Error(err))
	}
}

func (p *ProtoSystem) Pack(_ interface{}, args ...interface{}) (interface{}, interface{}, error) {
	return args, nil, nil
}

func (p *ProtoSystem) UnPack(_ interface{}, pack interface{}) ([]interface{}, interface{}, error) {
	return pack.([]interface{}), nil, nil
}

func (p *ProtoSystem) init() {
	p.cmds = make(map[string]ProtoHandler)
	p.Register("init", p.onInit)
	p.Register("kill", p.onKill)
	p.Register("exec", p.onExec)
	p.Register("fork", p.onFork)
}

func (p *ProtoSystem) onInit(msg *DispatchMessage, _ ...interface{}) error {
	a := msg.Actor.(*actorImpl)
	a.onInit()
	return nil
}

func (p *ProtoSystem) onKill(msg *DispatchMessage, _ ...interface{}) error {
	a := msg.Actor.(*actorImpl)
	a.onKill()
	return nil
}

func (p *ProtoSystem) onExec(msg *DispatchMessage, args ...interface{}) error {
	r, err := args[0].(ExecCallback)()
	msg.Response(err, r)
	return nil
}

func (p *ProtoSystem) onFork(_ *DispatchMessage, args ...interface{}) error {
	args[0].(ForkCallback)()
	return nil
}
