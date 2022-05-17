package go_actor

// NewProtoSystem 创建一个内部协议,处理init/kill/exec
func NewProtoSystem(logger Logger) *ProtoSystem {
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
						logger.Errorf("proto recovery cmd:%s args:%#v", msg.Headers.GetStr(HeaderIdMethod), args)
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

func (p *ProtoSystem) Register(cmd string, cb ProtoHandler, extend ...interface{}) {
	p.cmds[cmd] = cb
}

func (p *ProtoSystem) OnMessage(msg *DispatchMessage) {
	datas, _, _ := p.UnPack(nil, msg.Content)

	cmd := datas[0].(string)
	cb := p.cmds[cmd]
	if cb == nil {
		msg.Actor.Logger().Errorf("system message cmd:%s not exists\n", cmd)
		return
	}

	err := p.Trigger(cb, msg, datas[1:]...)
	if err != nil {
		msg.Actor.Logger().Errorf("system:%s msg error %s\n", cmd, err)
	}
}

func (p *ProtoSystem) Pack(ctx interface{}, args ...interface{}) (interface{}, interface{}, error) {
	return args, nil, nil
}

func (p *ProtoSystem) UnPack(ctx interface{}, pack interface{}) ([]interface{}, interface{}, error) {
	return pack.([]interface{}), nil, nil
}

func (p *ProtoSystem) init() {
	p.cmds = make(map[string]ProtoHandler)
	p.Register("init", p.onInit)
	p.Register("kill", p.onKill)
	p.Register("exec", p.onExec)
	p.Register("fork", p.onFork)
}

func (p *ProtoSystem) onInit(msg *DispatchMessage, args ...interface{}) error {
	a := msg.Actor.(*actorImpl)
	a.onInit()
	return nil
}

func (p *ProtoSystem) onKill(msg *DispatchMessage, args ...interface{}) error {
	a := msg.Actor.(*actorImpl)
	a.onKill()
	return nil
}

func (p *ProtoSystem) onExec(msg *DispatchMessage, args ...interface{}) error {
	/*
		f := reflect.ValueOf(args[1])
		rParams := make([]reflect.Value, len(args[2].([]interface{})))
		for i, p := range args[2].([]interface{}) {
			rParams[i] = reflect.ValueOf(p)
		}
		r := f.Call(rParams)
	*/

	session := args[0].(int)
	r, err := args[1].(ExecCallback)()
	msg.Actor.GetExecuter().OnResponse(session, err, r)
	return nil
}

func (p *ProtoSystem) onFork(msg *DispatchMessage, args ...interface{}) error {
	/*
		f := reflect.ValueOf(args[0])
		rParams := make([]reflect.Value, len(args[1].([]interface{})))
		for i, p := range args[1].([]interface{}) {
			rParams[i] = reflect.ValueOf(p)
		}
		f.Call(rParams)
	*/
	args[0].(ForkCallback)()
	return nil
}
