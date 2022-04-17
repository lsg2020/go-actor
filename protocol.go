package go_actor

import (
	message "github.com/lsg2020/go-actor/pb"
)

// 内部协议id
const (
	ProtocolResponse = 0xff // 回复消息id
	ProtocolSystem   = 0xfe // 内置系统消息id
)

// DispatchMessage actor收发消息的载体,并存储一些上下文信息
type DispatchMessage struct {
	System *ActorSystem
	Actor  Actor

	Headers     HeadersWrap
	Content     interface{}
	ResponseErr *ActorError

	RequestProto     Proto
	DispatchResponse func(msg *DispatchMessage, err *ActorError, data interface{})
}

// Response 回复通知消息的接口
func (msg *DispatchMessage) Response(err *ActorError, datas ...interface{}) {
	if err != nil {
		msg.DispatchResponse(msg, err, nil)
		return
	}
	requestProtoPackCtx := msg.Headers.GetInterface(HeaderIdRequestProtoPackCtx)
	rsp, _, err := msg.RequestProto.Pack(requestProtoPackCtx, datas...)
	if err != nil {
		msg.DispatchResponse(msg, err, nil)
		return
	}
	msg.DispatchResponse(msg, nil, rsp)
}

func (msg *DispatchMessage) Extract(headerIds ...int) Headers {
	var ret Headers
	for _, id := range headerIds {
		h := msg.Headers.Get(id)
		if h != nil {
			if ret == nil {
				ret = make(Headers, 0, len(headerIds))
			}
			ret = append(ret, *h)
		}
	}
	return ret
}

func (msg *DispatchMessage) ExtractEx(headerIds ...int) *CallOptions {
	headers := msg.Extract(headerIds...)
	if headers == nil {
		return nil
	}

	return &CallOptions{Headers: headers}
}

func (msg *DispatchMessage) FromPB(pb *message.Message) {
	msg.Headers.Cap(len(pb.Headers))
	for _, header := range pb.Headers {
		switch HeaderValType(header.Type) {
		case HeaderInt:
			msg.Headers.Put(BuildHeaderInt(int(header.Id), int(header.ValInt)))
		case HeaderString:
			msg.Headers.Put(BuildHeaderString(int(header.Id), header.ValString))
		case HeaderActorAddr:
			msg.Headers.Put(BuildHeaderActor(int(header.Id), &ActorAddr{
				NodeInstanceId: header.ValAddr.NodeInstanceId,
				Handle:         ActorHandle(header.ValAddr.Handle),
			}))
		case HeaderBytes:
			msg.Headers.Put(BuildHeaderBytes(int(header.Id), header.ValBytes))
		case HeaderInterface:
		}
	}

	msg.Content = pb.Payload
	if pb.ResponseErr != nil {
		msg.ResponseErr = &ActorError{
			Code: int(pb.ResponseErr.Code),
			Msg:  pb.ResponseErr.Msg,
		}
	}
}

func (msg *DispatchMessage) ToPB() *message.Message {
	pb := &message.Message{
		Headers: make([]*message.Header, 0, len(msg.Headers.headers)),
	}
	for i := 0; i < len(msg.Headers.headers); i++ {
		header := &msg.Headers.headers[i]
		if header.Private {
			continue
		}
		switch header.Type {
		case HeaderInt:
			pb.Headers = append(pb.Headers, &message.Header{
				Id:     int32(header.Id),
				Type:   int32(header.Type),
				ValInt: int32(header.ValInt),
			})
		case HeaderString:
			pb.Headers = append(pb.Headers, &message.Header{
				Id:        int32(header.Id),
				Type:      int32(header.Type),
				ValString: header.ValStr,
			})
		case HeaderActorAddr:
			pb.Headers = append(pb.Headers, &message.Header{
				Id:   int32(header.Id),
				Type: int32(header.Type),
				ValAddr: &message.ActorAddr{
					NodeInstanceId: header.ValAddr.NodeInstanceId,
					Handle:         uint32(header.ValAddr.Handle),
				},
			})
		case HeaderBytes:
			pb.Headers = append(pb.Headers, &message.Header{
				Id:       int32(header.Id),
				Type:     int32(header.Type),
				ValBytes: header.ValBytes,
			})
		case HeaderInterface:
		}
	}
	if msg.Content != nil {
		pb.Payload = msg.Content.([]byte)
	}
	if msg.ResponseErr != nil {
		pb.ResponseErr = &message.Error{
			Code: int32(msg.ResponseErr.Code),
			Msg:  msg.ResponseErr.Msg,
		}
	}
	return pb
}

// ProtoInterceptor 收发消息拦截器接口
type ProtoInterceptor func(msg *DispatchMessage, handler ProtoHandler, args ...interface{}) *ActorError

// ProtoInterceptorChain 合并多个拦截器,从第一个依次嵌套执行
func ProtoInterceptorChain(interceptors ...ProtoInterceptor) ProtoInterceptor {
	n := len(interceptors)

	if n > 1 {
		lastI := n - 1
		return func(msg *DispatchMessage, handler ProtoHandler, args ...interface{}) *ActorError {
			var (
				chainHandler ProtoHandler
				curI         int
			)

			chainHandler = func(msg *DispatchMessage, args ...interface{}) *ActorError {
				if curI == lastI {
					if handler == nil {
						return nil
					}
					return handler(msg, args...)
				}
				curI++
				err := interceptors[curI](msg, chainHandler, args...)
				curI--
				return err
			}

			return interceptors[0](msg, chainHandler, args...)
		}
	}

	if n == 1 {
		return interceptors[0]
	}

	return func(msg *DispatchMessage, handler ProtoHandler, args ...interface{}) *ActorError {
		if handler == nil {
			return nil
		}
		return handler(msg, args...)
	}
}

type ProtoHandler func(msg *DispatchMessage, args ...interface{}) *ActorError

// Proto 收发消息的打包/解包/注册分发
type Proto interface {
	Id() int
	Name() string
	OnMessage(msg *DispatchMessage)
	Pack(ctx interface{}, args ...interface{}) (interface{}, interface{}, *ActorError)
	UnPack(ctx interface{}, pack interface{}) ([]interface{}, interface{}, *ActorError)
	Register(name string, cb ProtoHandler, extend ...interface{})

	// InterceptorCall 消息发送的拦截器,方便设置自定义消息头/消息名称等信息
	InterceptorCall() ProtoInterceptor
	// InterceptorDispatch 消息处理的拦截器,方便记录指标/设置调用链/过滤消息/设置上下文等
	InterceptorDispatch() ProtoInterceptor
}

type ProtoBaseImpl struct {
	callInterceptor     ProtoInterceptor
	dispatchInterceptor ProtoInterceptor
}

func (p *ProtoBaseImpl) InterceptorCall() ProtoInterceptor {
	return p.callInterceptor
}

func (p *ProtoBaseImpl) InterceptorDispatch() ProtoInterceptor {
	return p.dispatchInterceptor
}

func (p *ProtoBaseImpl) Trigger(handler ProtoHandler, msg *DispatchMessage, args ...interface{}) *ActorError {
	if p.dispatchInterceptor == nil {
		return handler(msg, args...)
	}
	return p.dispatchInterceptor(msg, handler, args...)
}

type protoBaseOptions struct {
	calls     []ProtoInterceptor
	dispatchs []ProtoInterceptor
}

type ProtoOption func(ops *protoBaseOptions)

func ProtoBaseBuild(opts ...ProtoOption) ProtoBaseImpl {
	data := &protoBaseOptions{}
	for _, opt := range opts {
		opt(data)
	}

	ret := ProtoBaseImpl{}
	ret.callInterceptor = ProtoInterceptorChain(data.calls...)
	ret.dispatchInterceptor = ProtoInterceptorChain(data.dispatchs...)
	return ret
}

func ProtoWithInterceptorCall(interceptor ProtoInterceptor) ProtoOption {
	return func(ops *protoBaseOptions) {
		ops.calls = append(ops.calls, interceptor)
	}
}

func ProtoWithInterceptorDispatch(interceptor ProtoInterceptor) ProtoOption {
	return func(ops *protoBaseOptions) {
		ops.dispatchs = append(ops.dispatchs, interceptor)
	}
}
