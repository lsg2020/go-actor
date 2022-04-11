package protocols

import (
	"github.com/lsg2020/gactor"
)

func NewRaw(id int, opts ...gactor.ProtoOption) *Raw {
	return &Raw{
		ProtoBaseImpl: gactor.ProtoBaseBuild(opts...),
		protoId:       id,
		cmds:          make(map[string]gactor.ProtoHandler),
	}
}

type Raw struct {
	gactor.ProtoBaseImpl
	protoId int

	cmds map[string]gactor.ProtoHandler
}

func (raw *Raw) Id() int {
	return raw.protoId
}

func (raw *Raw) Name() string {
	return "raw"
}

func (raw *Raw) Register(cmd string, cb gactor.ProtoHandler, extend ...interface{}) {
	raw.cmds[cmd] = cb
}

func (raw *Raw) OnMessage(msg *gactor.DispatchMessage) {
	datas, _, _ := raw.UnPack(nil, msg.Content)
	msg.Content = datas[1:]

	cmd := datas[0].(string)
	cb := raw.cmds[cmd]
	if cb == nil { // nolint
		msg.System.Logger().Warnf("raw:%d cmd not exists %s\n", raw.protoId, cmd)
		return
	}

	raw.Trigger(cb, msg, msg.Content.([]interface{})...)
}

func (raw *Raw) Pack(ctx interface{}, args ...interface{}) (interface{}, interface{}, *gactor.ActorError) {
	return args, nil, nil
}

func (raw *Raw) UnPack(ctx interface{}, pack interface{}) ([]interface{}, interface{}, *gactor.ActorError) {
	return pack.([]interface{}), nil, nil
}
