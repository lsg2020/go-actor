package protocols

import goactor "github.com/lsg2020/go-actor"

func NewRaw(id int, opts ...goactor.ProtoOption) *Raw {
	return &Raw{
		ProtoBaseImpl: goactor.ProtoBaseBuild(opts...),
		protoId:       id,
		cmds:          make(map[string]goactor.ProtoHandler),
	}
}

type Raw struct {
	goactor.ProtoBaseImpl
	protoId int

	cmds map[string]goactor.ProtoHandler
}

func (raw *Raw) Id() int {
	return raw.protoId
}

func (raw *Raw) Name() string {
	return "raw"
}

func (raw *Raw) Register(cmd string, cb goactor.ProtoHandler, extend ...interface{}) {
	raw.cmds[cmd] = cb
}

func (raw *Raw) OnMessage(msg *goactor.DispatchMessage) {
	datas, _, _ := raw.UnPack(nil, msg.Content)
	msg.Content = datas[1:]

	cmd := datas[0].(string)
	cb := raw.cmds[cmd]
	if cb == nil { // nolint
		msg.Actor.Logger().Warnf("raw:%d cmd not exists %s\n", raw.protoId, cmd)
		return
	}

	err := raw.Trigger(cb, msg, msg.Content.([]interface{})...)
	if err != nil {
		msg.Actor.Logger().Warnf("raw:%s msg error %s\n", cmd, err)
	}
}

func (raw *Raw) Pack(ctx interface{}, args ...interface{}) (interface{}, interface{}, error) {
	return args, nil, nil
}

func (raw *Raw) UnPack(ctx interface{}, pack interface{}) ([]interface{}, interface{}, error) {
	return pack.([]interface{}), nil, nil
}
