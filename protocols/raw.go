package protocols

import (
	goactor "github.com/lsg2020/go-actor"
	"go.uber.org/zap"
)

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
	datas, _, _ := raw.UnPackRequest(msg.Content)
	msg.Content = datas[1:]

	cmd := datas[0].(string)
	cb := raw.cmds[cmd]
	if cb == nil { // nolint
		msg.Actor.Logger().Error("cmd not exists", zap.Int("proto_id", raw.protoId), zap.String("cmd", cmd))
		return
	}

	err := raw.Trigger(cb, msg, msg.Content.([]interface{})...)
	if err != nil {
		msg.Actor.Logger().Error("msg error", zap.Int("proto_id", raw.protoId), zap.String("cmd", cmd))
	}
}

func (raw *Raw) PackRequest(args ...interface{}) (interface{}, interface{}, error) {
	return args, nil, nil
}

func (raw *Raw) PackResponse(ctx interface{}, args ...interface{}) (interface{}, error) {
	return args, nil
}

func (raw *Raw) UnPackRequest(pack interface{}) ([]interface{}, interface{}, error) {
	return pack.([]interface{}), nil, nil
}

func (raw *Raw) UnPackResponse(ctx interface{}, pack interface{}) ([]interface{}, error) {
	return pack.([]interface{}), nil
}
