package goactor

import "go.uber.org/zap"

type ProtoAdmin struct {
	ProtoBaseImpl
	cmds map[string]ProtoHandler
}

func (p *ProtoAdmin) Id() int {
	return ProtocolAdmin
}

func (p *ProtoAdmin) Name() string {
	return "admin"
}

func (p *ProtoAdmin) GetOptions() []ProtoOption {
	return nil
}

func (p *ProtoAdmin) Register(cmd string, cb ProtoHandler, _ ...interface{}) {
	p.cmds[cmd] = cb
}

func (p *ProtoAdmin) OnMessage(msg *DispatchMessage) {
	datas, _, _ := p.UnPackRequest(msg.Content)

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

func (p *ProtoAdmin) PackRequest(args ...interface{}) (interface{}, interface{}, error) {
	return args, nil, nil
}

func (p *ProtoAdmin) UnPackRequest(pack interface{}) ([]interface{}, interface{}, error) {
	return pack.([]interface{}), nil, nil
}

func (p *ProtoAdmin) PackResponse(_ interface{}, args ...interface{}) (interface{}, error) {
	return args, nil
}

func (p *ProtoAdmin) UnPackResponse(_ interface{}, pack interface{}) ([]interface{}, error) {
	return pack.([]interface{}), nil
}

func (p *ProtoAdmin) Init() {
	p.cmds = make(map[string]ProtoHandler)
	p.Register("init", p.onInit)
	p.Register("kill", p.onKill)
	p.Register("exec", p.onExec)
	p.Register("fork", p.onFork)
}

func (p *ProtoAdmin) onInit(msg *DispatchMessage, _ ...interface{}) error {
	a := msg.Actor.(*actorImpl)
	a.onInit()
	return nil
}

func (p *ProtoAdmin) onKill(msg *DispatchMessage, _ ...interface{}) error {
	a := msg.Actor.(*actorImpl)
	a.onKill()
	return nil
}

func (p *ProtoAdmin) onExec(msg *DispatchMessage, args ...interface{}) error {
	r, err := args[0].(ExecCallback)()
	msg.Response(err, r)
	return nil
}

func (p *ProtoAdmin) onFork(_ *DispatchMessage, args ...interface{}) error {
	args[0].(ForkCallback)()
	return nil
}
