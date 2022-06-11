package protocols

import (
	"reflect"

	"github.com/golang/protobuf/proto"
	goactor "github.com/lsg2020/go-actor"
	"go.uber.org/zap"
)

// NewProtobuf 处理protobuf消息的打包/解包/注册分发
func NewProtobuf(id int, opts ...goactor.ProtoOption) *Protobuf {
	allopts := append(([]goactor.ProtoOption)(nil), goactor.ProtoWithInterceptorCall(func(msg *goactor.DispatchMessage, handler goactor.ProtoHandler, args ...interface{}) error {
		requestCtx := msg.Headers.GetInterface(goactor.HeaderIdRequestProtoPackCtx)
		msg.Headers.Put(goactor.BuildHeaderString(goactor.HeaderIdMethod, requestCtx.(*PbMethod).Name))
		return handler(msg, args...)
	}))
	allopts = append(allopts, opts...)

	proto := &Protobuf{
		ProtoBaseImpl: goactor.ProtoBaseBuild(allopts...),
		protoId:       id,
		cmds:          make(map[string]*PbMethod),
	}

	return proto
}

type PbMethod struct {
	Name string
	CB   goactor.ProtoHandler
	Req  func() proto.Message
	Rsp  func() proto.Message
}

type Protobuf struct {
	goactor.ProtoBaseImpl
	protoId int

	cmds map[string]*PbMethod
}

func (p *Protobuf) Id() int {
	return p.protoId
}

func (p *Protobuf) Name() string {
	return "protobuf"
}

func (p *Protobuf) Register(cmd string, cb goactor.ProtoHandler, extend ...interface{}) {
	p.cmds[cmd] = &PbMethod{
		Name: cmd,
		CB:   cb,
		Req:  extend[0].(func() proto.Message),
		Rsp:  extend[1].(func() proto.Message),
	}
}

func (p *Protobuf) OnMessage(msg *goactor.DispatchMessage) {
	datas, responseCtx, err := p.UnPack(nil, msg)
	if err != nil {
		if msg.Headers.GetInt(goactor.HeaderIdSession) != 0 {
			msg.Response(err)
		} else {
			msg.Actor.Logger().Error("proto unpack error", zap.Int("proto_id", p.protoId), zap.Error(err))
		}
		return
	}
	msg.Headers.Put(goactor.BuildHeaderInterfaceRaw(goactor.HeaderIdRequestProtoPackCtx, responseCtx, true))
	cmd := responseCtx.(*PbMethod)
	msg.Content = datas

	err = p.Trigger(cmd.CB, msg, msg.Content.([]interface{})...)
	if err != nil {
		msg.Actor.Logger().Error("proto msg error", zap.String("cmd", cmd.Name), zap.Error(err))
	}
}

func (p *Protobuf) Pack(ctx interface{}, args ...interface{}) (interface{}, interface{}, error) {
	if ctx != nil {
		// response msg
		if len(args) != 1 {
			return nil, nil, goactor.ErrorWrapf(goactor.ErrPackErr, "protocol:%d args len error %d", p.protoId, len(args))
		}

		msg, ok := args[0].(proto.Message)
		if !ok {
			return nil, nil, goactor.ErrorWrapf(goactor.ErrPackErr, "protocol:%d type err %s", p.protoId, reflect.TypeOf(args[0]).String())
		}
		buf, err := proto.Marshal(msg)
		if err != nil {
			return nil, nil, goactor.ErrorWrapf(err, "protocol:%d marshal error %s", p.protoId, msg.String())
		}
		return buf, nil, nil
	}

	// request msg
	if len(args) != 2 {
		return nil, nil, goactor.ErrorWrapf(goactor.ErrPackErr, "protocol:%d params len err %d", p.protoId, len(args))
	}

	method, ok := args[0].(string)
	if !ok {
		return nil, nil, goactor.ErrorWrapf(goactor.ErrPackErr, "protocol:%d type err %s", p.protoId, reflect.TypeOf(args[0]).String())
	}
	msg, ok := args[1].(proto.Message)
	if !ok {
		return nil, nil, goactor.ErrorWrapf(goactor.ErrPackErr, "protocol:%d type err %s", p.protoId, reflect.TypeOf(args[1]).String())
	}
	buf, err := proto.Marshal(msg)
	if err != nil {
		return nil, nil, goactor.ErrorWrapf(err, "protocol:%d marshal error %s", p.protoId, msg.String())
	}

	cmd := p.cmds[method]
	if cmd == nil {
		return nil, nil, goactor.ErrorWrapf(goactor.ErrCmdNotExists, "protocol:%d method:%s", p.protoId, method)
	}

	return buf, cmd, nil
}

func (p *Protobuf) UnPack(ctx interface{}, args interface{}) ([]interface{}, interface{}, error) {
	if ctx != nil {
		var buf []byte
		if args != nil {
			var ok bool
			buf, ok = args.([]byte)
			if !ok {
				return nil, nil, goactor.ErrorWrapf(goactor.ErrPackErr, "protocol:%d content type error %s", p.protoId, reflect.TypeOf(args).String())
			}
		}

		// response msg
		rsp := ctx.(*PbMethod).Rsp()
		err := proto.Unmarshal(buf, rsp)
		if err != nil {
			return nil, nil, goactor.ErrorWrapf(err, "protocol:%d unmarshal error %s", p.protoId, rsp.String())
		}
		return []interface{}{rsp}, nil, nil
	}

	msg, ok := args.(*goactor.DispatchMessage)
	if !ok {
		return nil, nil, goactor.ErrorWrapf(goactor.ErrPackErr, "protocol:%d type error %s", p.protoId, reflect.TypeOf(args).String())
	}
	var buf []byte
	if msg.Content != nil {
		var ok bool
		buf, ok = msg.Content.([]byte)
		if !ok {
			return nil, nil, goactor.ErrorWrapf(goactor.ErrPackErr, "protocol:%d type error %s", p.protoId, reflect.TypeOf(msg.Content).String())
		}
	}

	cmd := p.cmds[msg.Headers.GetStr(goactor.HeaderIdMethod)]
	if cmd == nil {
		return nil, nil, goactor.ErrorWrapf(goactor.ErrCmdNotExists, "protocol:%d method: %s", p.protoId, msg.Headers.GetStr(goactor.HeaderIdMethod))
	}

	req := cmd.Req()
	err := proto.Unmarshal(buf, req)
	if err != nil {
		return nil, nil, goactor.ErrorWrapf(goactor.ErrPackErr, "protocol:%d unmarshal error %s", p.protoId, req.String())
	}
	return []interface{}{req}, cmd, nil
}
