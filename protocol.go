package gactor

import (
	message "github.com/lsg2020/gactor/pb"
)

const (
	ProtocolResponse = 0xff
	ProtocolSystem   = 0xfe
)

type DispatchMessage struct {
	System *ActorSystem
	Actor  Actor

	Headers     HeadersWrap
	Content     interface{}
	ResponseErr *ActorError

	RequestProto     Proto
	DispatchResponse func(msg *DispatchMessage, err *ActorError, data interface{})
}

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

type ProtoInterceptor func(msg *DispatchMessage, handler ProtoHandler, args ...interface{})

func ProtoInterceptorChain(interceptors ...ProtoInterceptor) ProtoInterceptor {
	n := len(interceptors)

	if n > 1 {
		lastI := n - 1
		return func(msg *DispatchMessage, handler ProtoHandler, args ...interface{}) {
			var (
				chainHandler ProtoHandler
				curI         int
			)

			chainHandler = func(msg *DispatchMessage, args ...interface{}) {
				if curI == lastI {
					if handler == nil {
						return
					}
					handler(msg, args...)
					return
				}
				curI++
				interceptors[curI](msg, chainHandler, args...)
				curI--
			}

			interceptors[0](msg, chainHandler, args...)
		}
	}

	if n == 1 {
		return interceptors[0]
	}

	return func(msg *DispatchMessage, handler ProtoHandler, args ...interface{}) {
		if handler == nil {
			return
		}
		handler(msg, args...)
	}
}

type ProtoHandler func(msg *DispatchMessage, args ...interface{})

type Proto interface {
	Id() int
	Name() string
	OnMessage(msg *DispatchMessage)
	Pack(ctx interface{}, args ...interface{}) (interface{}, interface{}, *ActorError)
	UnPack(ctx interface{}, pack interface{}) ([]interface{}, interface{}, *ActorError)
	Register(name string, cb ProtoHandler, extend ...interface{})

	InterceptorSend() ProtoInterceptor
	InterceptorRequest() ProtoInterceptor
}

type ProtoBaseImpl struct {
	sendInterceptor    ProtoInterceptor
	requestInterceptor ProtoInterceptor
}

func (p *ProtoBaseImpl) InterceptorSend() ProtoInterceptor {
	return p.sendInterceptor
}

func (p *ProtoBaseImpl) InterceptorRequest() ProtoInterceptor {
	return p.requestInterceptor
}

func (p *ProtoBaseImpl) Trigger(handler ProtoHandler, msg *DispatchMessage, args ...interface{}) {
	if p.requestInterceptor == nil {
		handler(msg, args...)
		return
	}
	p.requestInterceptor(msg, handler, args...)
}

type protoBaseOptions struct {
	sends    []ProtoInterceptor
	requests []ProtoInterceptor
}

type ProtoOption func(ops *protoBaseOptions)

func ProtoBaseBuild(opts ...ProtoOption) ProtoBaseImpl {
	data := &protoBaseOptions{}
	for _, opt := range opts {
		opt(data)
	}

	ret := ProtoBaseImpl{}
	ret.sendInterceptor = ProtoInterceptorChain(data.sends...)
	ret.requestInterceptor = ProtoInterceptorChain(data.requests...)
	return ret
}

func ProtoWithInterceptorSend(interceptor ProtoInterceptor) ProtoOption {
	return func(ops *protoBaseOptions) {
		ops.sends = append(ops.sends, interceptor)
	}
}

func ProtoWithInterceptorRequest(interceptor ProtoInterceptor) ProtoOption {
	return func(ops *protoBaseOptions) {
		ops.requests = append(ops.requests, interceptor)
	}
}
