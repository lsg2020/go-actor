package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"time"

	go_actor "github.com/lsg2020/go-actor"
	hello "github.com/lsg2020/go-actor/examples/pb"
	"github.com/lsg2020/go-actor/executer"
	"github.com/lsg2020/go-actor/protocols"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

var system *go_actor.ActorSystem
var client *hello.HelloServiceClient
var selector go_actor.Selector

type HelloActor struct {
	actor go_actor.Actor
	addr  *go_actor.ActorAddr
}

func (hello *HelloActor) OnInit(a go_actor.Actor) {
	hello.actor = a
	hello.addr = system.Register(a, "hello")
	log.Println("actor init", *hello.addr)
}

func (hello *HelloActor) OnRelease(a go_actor.Actor) {

}

type HelloService struct {
}

func (s *HelloService) OnSend(ctx *go_actor.DispatchMessage, req *hello.Request) *go_actor.ActorError {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnSend in actor:%#v req:%#v\n", a.addr, req.String())
	return nil
}

func (s *HelloService) OnAdd(ctx *go_actor.DispatchMessage, req *hello.Request) (*hello.Response, *go_actor.ActorError) {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnAdd in actor:%#v req:%#v\n", a.addr, req.String())
	return &hello.Response{R: req.A + req.B}, nil
}

func (s *HelloService) OnTestCallAdd(ctx *go_actor.DispatchMessage, req *hello.Request) (*hello.Response, *go_actor.ActorError) {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnCallSourceAdd in actor:%#v req:%#v\n", a.addr, req.String())

	rsp, err := client.Add(system.Context(), system, a.actor, selector.Addr(), req, ctx.ExtractEx(go_actor.HeaderIdTracingSpan))
	return rsp, err
}

type ClientActor struct {
}

func (c *ClientActor) OnInit(a go_actor.Actor) {
	for i := int32(0); true; i++ {
		client.Send(system, a, selector.Addr(), &hello.Request{A: i, B: 10}, nil)
		rsp, err := client.TestCallAdd(system.Context(), system, a, selector.Addr(), &hello.Request{A: i, B: 10}, nil)
		log.Println("test call resource", rsp, err)
		a.Sleep(time.Second)
	}
}

func (c *ClientActor) OnRelease(a go_actor.Actor) {
}

// NewTracer 创建一个jaeger Tracer
func NewTracer(serviceName, addr string) (opentracing.Tracer, io.Closer, error) {
	cfg := jaegercfg.Configuration{
		ServiceName: serviceName,
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans:            true,
			BufferFlushInterval: 1 * time.Second,
		},
	}

	sender, err := jaeger.NewUDPTransport(addr, 0)
	if err != nil {
		return nil, nil, err
	}

	reporter := jaeger.NewRemoteReporter(sender)
	// Initialize tracer with a logger and a metrics factory
	tracer, closer, err := cfg.NewTracer(
		jaegercfg.Logger(jaeger.StdLogger),
		jaegercfg.Reporter(reporter),
	)

	return tracer, closer, err
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	var err error
	system, err = go_actor.NewActorSystem(
		go_actor.WithName("tracing"),
		go_actor.WithInstanceId(1),
		go_actor.WithEtcd("http://10.21.248.213:2379"),
		go_actor.WithContext(ctx),
	)
	if err != nil {
		panic(err)
	}

	single := &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)

	tracer, _, err := NewTracer("hello_1", "10.21.248.28:5775")
	if err != nil {
		panic(err)
	}
	opentracing.SetGlobalTracer(tracer)

	proto := protocols.NewProtobuf(1, go_actor.ProtoWithInterceptorCall(func(msg *go_actor.DispatchMessage, handler go_actor.ProtoHandler, args ...interface{}) *go_actor.ActorError {
		method := msg.Headers.GetStr(go_actor.HeaderIdMethod)
		span := msg.Headers.GetInterface(go_actor.HeaderIdTracingSpan)
		var spanContext opentracing.Span
		if span == nil {
			spanContext = tracer.StartSpan(method)
		} else {
			spanContext = tracer.StartSpan(method, opentracing.ChildOf(span.(opentracing.Span).Context()))
		}
		defer spanContext.Finish()

		carrier := new(bytes.Buffer)
		tracer.Inject(spanContext.Context(), opentracing.Binary, carrier)
		msg.Headers.Put(go_actor.BuildHeaderBytes(go_actor.HeaderIdTracingSpanCarrier, carrier.Bytes()))

		return handler(msg, args...)
	}), go_actor.ProtoWithInterceptorDispatch(func(msg *go_actor.DispatchMessage, handler go_actor.ProtoHandler, args ...interface{}) *go_actor.ActorError {
		spanCarrier := msg.Headers.GetBytes(go_actor.HeaderIdTracingSpanCarrier)
		spanContext, err := tracer.Extract(opentracing.Binary, bytes.NewBuffer(spanCarrier))
		if err != nil {
			return go_actor.ErrorWrap(err)
		}
		span := tracer.StartSpan("operation", opentracing.ChildOf(spanContext))
		defer span.Finish()
		msg.Headers.Put(go_actor.BuildHeaderInterfaceRaw(go_actor.HeaderIdTracingSpan, span, true))

		return handler(msg, args...)
	}))
	hello.RegisterHelloService(&HelloService{}, proto)
	client = hello.NewHelloServiceClient(proto)

	selector, err = go_actor.NewRandomSelector(system, "hello")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		go_actor.NewActor(&HelloActor{}, single, go_actor.ActorWithProto(proto))
	}

	go_actor.NewActor(&ClientActor{}, single, go_actor.ActorWithProto(proto))

	select {
	case <-system.Context().Done():
	}
}
