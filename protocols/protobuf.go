package protocols

import (
	"fmt"

	"github.com/lsg2020/gactor"
	"google.golang.org/protobuf/proto"
)

func NewProtobuf(id int, opts ...gactor.ProtoOption) *Protobuf {
	opts = append(opts, gactor.ProtoWithInterceptorSend(func(msg *gactor.DispatchMessage, handler gactor.ProtoHandler, args ...interface{}) {
		requestCtx := msg.Headers.GetInterface(gactor.HeaderIdRequestProtoPackCtx)
		msg.Headers.Put(gactor.BuildHeaderString(gactor.HeaderIdMethod, requestCtx.(*PbMethod).Name))
	}))

	proto := &Protobuf{
		ProtoBaseImpl: gactor.ProtoBaseBuild(opts...),
		protoId:       id,
		cmds:          make(map[string]*PbMethod),
	}

	return proto
}

type PbMethod struct {
	Name string
	CB   gactor.ProtoHandler
	Req  func() proto.Message
	Rsp  func() proto.Message
}

type Protobuf struct {
	gactor.ProtoBaseImpl
	protoId int

	cmds map[string]*PbMethod
}

func (p *Protobuf) Id() int {
	return p.protoId
}

func (p *Protobuf) Name() string {
	return "protobuf"
}

func (p *Protobuf) Register(cmd string, cb gactor.ProtoHandler, extend ...interface{}) {
	p.cmds[cmd] = &PbMethod{
		Name: cmd,
		CB:   cb,
		Req:  extend[0].(func() proto.Message),
		Rsp:  extend[1].(func() proto.Message),
	}
}

func (p *Protobuf) OnMessage(msg *gactor.DispatchMessage) {
	datas, responseCtx, err := p.UnPack(nil, msg)
	if err != nil {
		if msg.Headers.GetInt(gactor.HeaderIdSession) != 0 {
			msg.Response(err)
		} else {
			msg.System.Logger().Errorf("proto %d unpack error:%s\n", p.protoId, err.Error())
		}
		return
	}
	msg.Headers.Put(gactor.BuildHeaderInterfaceRaw(gactor.HeaderIdRequestProtoPackCtx, responseCtx, true))
	cmd := responseCtx.(*PbMethod)
	msg.Content = datas

	p.Trigger(cmd.CB, msg, msg.Content.([]interface{})...)
}

func (p *Protobuf) Pack(ctx interface{}, args ...interface{}) (interface{}, interface{}, *gactor.ActorError) {
	if ctx != nil {
		// response msg
		if len(args) != 1 {
			return nil, nil, gactor.ErrorActor(gactor.ErrCodePackErr, "params len err")
		}

		msg, ok := args[0].(proto.Message)
		if !ok {
			return nil, nil, gactor.ErrorActor(gactor.ErrCodePackErr, "type err")
		}
		buf, err := proto.Marshal(msg)
		if err != nil {
			return nil, nil, gactor.ErrorActor(gactor.ErrCodePackErr, err.Error())
		}
		return buf, nil, nil
	}

	// request msg
	if len(args) != 2 {
		return nil, nil, gactor.ErrorActor(gactor.ErrCodePackErr, "params len err")
	}

	method, ok := args[0].(string)
	if !ok {
		return nil, nil, gactor.ErrorActor(gactor.ErrCodePackErr, "type err")
	}
	msg, ok := args[1].(proto.Message)
	if !ok {
		return nil, nil, gactor.ErrorActor(gactor.ErrCodePackErr, "type err")
	}
	buf, err := proto.Marshal(msg)
	if err != nil {
		return nil, nil, gactor.ErrorActor(gactor.ErrCodePackErr, err.Error())
	}

	cmd := p.cmds[method]
	if cmd == nil {
		return nil, nil, gactor.ErrorActor(gactor.ErrCodeCmdNotExists, fmt.Sprintf("method:%s not exists", method))
	}

	return buf, cmd, nil
}

func (p *Protobuf) UnPack(ctx interface{}, args interface{}) ([]interface{}, interface{}, *gactor.ActorError) {
	if ctx != nil {
		var buf []byte
		if args != nil {
			var ok bool
			buf, ok = args.([]byte)
			if !ok {
				return nil, nil, gactor.ErrorActor(gactor.ErrCodePackErr, "content err")
			}
		}

		// response msg
		rsp := ctx.(*PbMethod).Rsp()
		err := proto.Unmarshal(buf, rsp)
		if err != nil {
			return nil, nil, gactor.ErrorActor(gactor.ErrCodePackErr, err.Error())
		}
		return []interface{}{rsp}, nil, nil
	}

	msg, ok := args.(*gactor.DispatchMessage)
	if !ok {
		return nil, nil, gactor.ErrorActor(gactor.ErrCodePackErr, "type err")
	}
	var buf []byte
	if msg.Content != nil {
		var ok bool
		buf, ok = msg.Content.([]byte)
		if !ok {
			return nil, nil, gactor.ErrorActor(gactor.ErrCodePackErr, "content err")
		}
	}

	cmd := p.cmds[msg.Headers.GetStr(gactor.HeaderIdMethod)]
	if cmd == nil {
		return nil, nil, gactor.ErrorActor(gactor.ErrCodeCmdNotExists, "method not exists: "+msg.Headers.GetStr(gactor.HeaderIdMethod))
	}

	req := cmd.Req()
	err := proto.Unmarshal(buf, req)
	if err != nil {
		return nil, nil, gactor.ErrorActor(gactor.ErrCodePackErr, err.Error())
	}
	return []interface{}{req}, cmd, nil
}
