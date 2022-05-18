package protocols

import (
	"fmt"

	goactor "github.com/lsg2020/go-actor"
	"google.golang.org/protobuf/proto"
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
			msg.Actor.Logger().Errorf("proto %d unpack error:%s\n", p.protoId, err.Error())
		}
		return
	}
	msg.Headers.Put(goactor.BuildHeaderInterfaceRaw(goactor.HeaderIdRequestProtoPackCtx, responseCtx, true))
	cmd := responseCtx.(*PbMethod)
	msg.Content = datas

	err = p.Trigger(cmd.CB, msg, msg.Content.([]interface{})...)
	if err != nil {
		msg.Actor.Logger().Errorf("proto:%s msg error %s\n", cmd.Name, err)
	}
}

func (p *Protobuf) Pack(ctx interface{}, args ...interface{}) (interface{}, interface{}, error) {
	if ctx != nil {
		// response msg
		if len(args) != 1 {
			return nil, nil, goactor.ErrorActor(goactor.ErrCodePackErr, "params len err")
		}

		msg, ok := args[0].(proto.Message)
		if !ok {
			return nil, nil, goactor.ErrorActor(goactor.ErrCodePackErr, "type err")
		}
		buf, err := proto.Marshal(msg)
		if err != nil {
			return nil, nil, goactor.ErrorActor(goactor.ErrCodePackErr, err.Error())
		}
		return buf, nil, nil
	}

	// request msg
	if len(args) != 2 {
		return nil, nil, goactor.ErrorActor(goactor.ErrCodePackErr, "params len err")
	}

	method, ok := args[0].(string)
	if !ok {
		return nil, nil, goactor.ErrorActor(goactor.ErrCodePackErr, "type err")
	}
	msg, ok := args[1].(proto.Message)
	if !ok {
		return nil, nil, goactor.ErrorActor(goactor.ErrCodePackErr, "type err")
	}
	buf, err := proto.Marshal(msg)
	if err != nil {
		return nil, nil, goactor.ErrorActor(goactor.ErrCodePackErr, err.Error())
	}

	cmd := p.cmds[method]
	if cmd == nil {
		return nil, nil, goactor.ErrorActor(goactor.ErrCodeCmdNotExists, fmt.Sprintf("method:%s not exists", method))
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
				return nil, nil, goactor.ErrorActor(goactor.ErrCodePackErr, "content err")
			}
		}

		// response msg
		rsp := ctx.(*PbMethod).Rsp()
		err := proto.Unmarshal(buf, rsp)
		if err != nil {
			return nil, nil, goactor.ErrorActor(goactor.ErrCodePackErr, err.Error())
		}
		return []interface{}{rsp}, nil, nil
	}

	msg, ok := args.(*goactor.DispatchMessage)
	if !ok {
		return nil, nil, goactor.ErrorActor(goactor.ErrCodePackErr, "type err")
	}
	var buf []byte
	if msg.Content != nil {
		var ok bool
		buf, ok = msg.Content.([]byte)
		if !ok {
			return nil, nil, goactor.ErrorActor(goactor.ErrCodePackErr, "content err")
		}
	}

	cmd := p.cmds[msg.Headers.GetStr(goactor.HeaderIdMethod)]
	if cmd == nil {
		return nil, nil, goactor.ErrorActor(goactor.ErrCodeCmdNotExists, "method not exists: "+msg.Headers.GetStr(goactor.HeaderIdMethod))
	}

	req := cmd.Req()
	err := proto.Unmarshal(buf, req)
	if err != nil {
		return nil, nil, goactor.ErrorActor(goactor.ErrCodePackErr, err.Error())
	}
	return []interface{}{req}, cmd, nil
}
