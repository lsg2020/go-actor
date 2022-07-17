package goactor

import (
	"context"

	message "github.com/lsg2020/go-actor/pb"
)

// 内部协议id
const (
	ProtocolResponse = 0xff // 回复消息id
	ProtocolAdmin    = 0xfe // 内置系统消息id
)

func NewDispatchMessage(
	ctx context.Context,
	actor Actor,
	destination *ActorAddr,
	protocol Proto,
	protocolCtx interface{},
	protocolId int,
	sessionId int,
	headers Headers,
	content interface{},
	responseErr error,
	response DispatchResponseCB,
) *DispatchMessage {
	msg := &DispatchMessage{
		Actor: actor,

		Content:     content,
		ResponseErr: responseErr,

		Protocol:         protocol,
		ProtocolCtx:      protocolCtx,
		DispatchResponse: response,
		ProtocolId:       protocolId,
		SessionId:        sessionId,
		Destination:      destination,
		ctx:              ctx,
	}
	if headers != nil {
		msg.Headers = HeadersWrap{headers}
	}

	return msg
}

func NewDispatchMessageFromPB(data *message.Message, response DispatchResponseCB) *DispatchMessage {
	msg := &DispatchMessage{
		DispatchResponse: response,
	}
	msg.FromPB(data)
	msg.Init()
	return msg
}

type DispatchResponseCB func(msg *DispatchMessage, err error, data interface{})

// DispatchMessage actor收发消息的载体,并存储一些上下文信息
type DispatchMessage struct {
	System *ActorSystem
	Actor  Actor

	Headers     HeadersWrap
	Content     interface{}
	ResponseErr error

	Protocol         Proto
	DispatchResponse DispatchResponseCB
	ProtocolCtx      interface{}
	SessionId        int
	ProtocolId       int
	Destination      *ActorAddr
	ctx              context.Context
}

func (msg *DispatchMessage) Init() {
	if msg.SessionId == 0 {
		msg.SessionId = msg.Headers.GetInt(HeaderIdSession)
	}
	if msg.ProtocolId == 0 {
		msg.ProtocolId = msg.Headers.GetInt(HeaderIdProtocol)
	}
	if msg.Destination == nil {
		msg.Destination = msg.Headers.GetAddr(HeaderIdDestination)
	}
}

func (msg *DispatchMessage) Context() context.Context {
	if msg.ctx != nil {
		return msg.ctx
	}
	if msg.Actor != nil {
		return msg.Actor.Context()
	}
	return context.TODO()
}

func (msg *DispatchMessage) SetContext(ctx context.Context) {
	msg.ctx = ctx
}

func (msg *DispatchMessage) Session() int {
	return msg.SessionId
}

// Response 回复通知消息的接口
func (msg *DispatchMessage) Response(err error, datas ...interface{}) {
	if msg.DispatchResponse == nil {
		return
	}
	if err != nil && msg.Protocol != nil {
		_ = msg.Protocol.InterceptorError()(msg, nil, err)
	}

	if err != nil {
		msg.DispatchResponse(msg, err, nil)
		msg.DispatchResponse = nil
		return
	}
	rsp, err := msg.Protocol.PackResponse(msg.ProtocolCtx, datas...)
	if err != nil {
		msg.DispatchResponse(msg, err, nil)
		msg.DispatchResponse = nil
		return
	}
	msg.DispatchResponse(msg, nil, rsp)
	msg.DispatchResponse = nil
}

func (msg *DispatchMessage) MaybeResponseErr(err error) {
	if msg.SessionId == 0 {
		return
	}
	msg.Response(err, nil)
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
		case HeaderUint64:
			msg.Headers.Put(BuildHeaderUint64(int(header.Id), header.ValUint64))
		case HeaderInt64:
			msg.Headers.Put(BuildHeaderInt64(int(header.Id), header.ValInt64))
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
		case HeaderUint64:
			pb.Headers = append(pb.Headers, &message.Header{
				Id:        int32(header.Id),
				Type:      int32(header.Type),
				ValUint64: header.ValUint64,
			})
		case HeaderInt64:
			pb.Headers = append(pb.Headers, &message.Header{
				Id:       int32(header.Id),
				Type:     int32(header.Type),
				ValInt64: header.ValInt64,
			})
		}
	}
	if msg.Content != nil {
		pb.Payload = msg.Content.([]byte)
	}
	if msg.ResponseErr != nil {
		if err, ok := msg.ResponseErr.(*ActorError); ok {
			pb.ResponseErr = &message.Error{
				Code: int32(err.Code),
				Msg:  err.Msg,
			}
		} else {
			pb.ResponseErr = &message.Error{
				Code: int32(ErrCodeSystem),
				Msg:  msg.ResponseErr.Error(),
			}
		}
	}
	return pb
}

// ProtoInterceptor 收发消息拦截器接口
type ProtoInterceptor func(msg *DispatchMessage, handler ProtoHandler, args ...interface{}) error

// ProtoInterceptorChain 合并多个拦截器,从第一个依次嵌套执行
func ProtoInterceptorChain(interceptors ...ProtoInterceptor) ProtoInterceptor {
	n := len(interceptors)

	if n > 1 {
		lastI := n - 1
		return func(msg *DispatchMessage, handler ProtoHandler, args ...interface{}) error {
			var (
				chainHandler ProtoHandler
				curI         int
			)

			chainHandler = func(msg *DispatchMessage, args ...interface{}) error {
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

	return func(msg *DispatchMessage, handler ProtoHandler, args ...interface{}) error {
		if handler == nil {
			return nil
		}
		return handler(msg, args...)
	}
}

type ProtoHandler func(msg *DispatchMessage, args ...interface{}) error

// Proto 收发消息的打包/解包/注册分发
type Proto interface {
	Id() int
	Name() string
	OnMessage(msg *DispatchMessage)
	Register(name string, cb ProtoHandler, extend ...interface{})

	PackRequest(args ...interface{}) (interface{}, interface{}, error)       // returns: (pack, responseCtx, error)
	UnPackRequest(pack interface{}) ([]interface{}, interface{}, error)      // returns: (args, responseCtx, error)
	PackResponse(ctx interface{}, args ...interface{}) (interface{}, error)  // returns: (pack, error)
	UnPackResponse(ctx interface{}, pack interface{}) ([]interface{}, error) // returns: (args, error)

	// InterceptorCall 消息发送的拦截器,方便设置自定义消息头/消息名称等信息
	InterceptorCall() ProtoInterceptor
	// InterceptorDispatch 消息处理的拦截器,方便记录指标/设置调用链/过滤消息/设置上下文等
	InterceptorDispatch() ProtoInterceptor
	// InterceptorError 消息错误的拦截器,方便统计error
	InterceptorError() ProtoInterceptor
}

type ProtoBaseImpl struct {
	callInterceptor     ProtoInterceptor
	dispatchInterceptor ProtoInterceptor
	errorInterceptor    ProtoInterceptor
}

func (p *ProtoBaseImpl) InterceptorCall() ProtoInterceptor {
	return p.callInterceptor
}

func (p *ProtoBaseImpl) InterceptorDispatch() ProtoInterceptor {
	return p.dispatchInterceptor
}

func (p *ProtoBaseImpl) InterceptorError() ProtoInterceptor {
	return p.errorInterceptor
}

func (p *ProtoBaseImpl) Trigger(handler ProtoHandler, msg *DispatchMessage, args ...interface{}) error {
	if p.dispatchInterceptor == nil {
		return handler(msg, args...)
	}
	err := p.dispatchInterceptor(msg, handler, args...)
	if err != nil {
		_ = p.errorInterceptor(msg, nil, err)
	}
	return err
}

type protoBaseOptions struct {
	calls     []ProtoInterceptor
	dispatchs []ProtoInterceptor
	errors    []ProtoInterceptor
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
	ret.errorInterceptor = ProtoInterceptorChain(data.errors...)
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

func ProtoWithInterceptorError(interceptor ProtoInterceptor) ProtoOption {
	return func(ops *protoBaseOptions) {
		ops.errors = append(ops.errors, interceptor)
	}
}
