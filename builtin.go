package gactor

import "reflect"

func NewProtoSystem(logger Logger) *ProtoSystem {
	p := &ProtoSystem{
		ProtoBaseImpl: ProtoBaseBuild(
			ProtoWithInterceptorSend(func(msg *DispatchMessage, handler ProtoHandler, args ...interface{}) {
				msg.Headers.Put(BuildHeaderString(HeaderIdMethod, msg.Content.([]interface{})[0].(string)))
			}),
			ProtoWithInterceptorRequest(func(msg *DispatchMessage, handler ProtoHandler, args ...interface{}) {
				defer func() {
					if r := recover(); r != nil {
						logger.Errorf("proto recovery cmd:%s args:%#v", msg.Headers.GetStr(HeaderIdMethod), args)
					}
				}()
				handler(msg, args...)
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

func (p *ProtoSystem) Register(cmd string, cb ProtoHandler, extend ...interface{}) {
	p.cmds[cmd] = cb
}

func (p *ProtoSystem) OnMessage(msg *DispatchMessage) {
	datas, _, _ := p.UnPack(nil, msg.Content)

	cmd := datas[0].(string)
	cb := p.cmds[cmd]
	if cb != nil {
		p.Trigger(cb, msg, datas[1:]...)
	}
}

func (p *ProtoSystem) Pack(ctx interface{}, args ...interface{}) (interface{}, interface{}, *ActorError) {
	return args, nil, nil
}

func (p *ProtoSystem) UnPack(ctx interface{}, pack interface{}) ([]interface{}, interface{}, *ActorError) {
	return pack.([]interface{}), nil, nil
}

func (p *ProtoSystem) init() {
	p.cmds = make(map[string]ProtoHandler)
	p.Register("init", p.onInit)
	p.Register("kill", p.onKill)
	p.Register("exec", p.onExec)
}

func (p *ProtoSystem) onInit(msg *DispatchMessage, args ...interface{}) {
	defer func() {
		if r := recover(); r != nil {
			msg.Actor.Logger().Errorf("actor start error %#v", r)
			msg.Actor.Kill()
		}
	}()
	a := msg.Actor.(*actorImpl)
	a.Instance().OnInit(a)

	if a.ops.initcb != nil {
		a.ops.initcb()
	}
}

func (p *ProtoSystem) onKill(msg *DispatchMessage, args ...interface{}) {
	a := msg.Actor.(*actorImpl)
	a.onKill()
}

func (p *ProtoSystem) onExec(msg *DispatchMessage, args ...interface{}) {
	f := reflect.ValueOf(args[0])

	rParams := make([]reflect.Value, len(args[1].([]interface{})))
	for i, p := range args[1].([]interface{}) {
		rParams[i] = reflect.ValueOf(p)
	}
	f.Call(rParams)
}
