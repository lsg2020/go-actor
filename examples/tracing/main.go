package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"time"

	goactor "github.com/lsg2020/go-actor"
	hello "github.com/lsg2020/go-actor/examples/pb"
	"github.com/lsg2020/go-actor/executer"
	"github.com/lsg2020/go-actor/protocols"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

var system *goactor.ActorSystem
var client *hello.HelloServiceClient
var selector goactor.Selector

type HelloActor struct {
	actor goactor.Actor
	addr  *goactor.ActorAddr
}

func (hello *HelloActor) OnInit(a goactor.Actor) {
	hello.actor = a
	hello.addr = system.Register(a, "hello")
	log.Println("actor init", *hello.addr)
}

func (hello *HelloActor) OnRelease(a goactor.Actor) {

}

type HelloService struct {
}

func (s *HelloService) OnSend(ctx *goactor.DispatchMessage, req *hello.Request) error {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnSend in actor:%#v req:%#v\n", a.addr, req.String())
	return nil
}

func (s *HelloService) OnAdd(ctx *goactor.DispatchMessage, req *hello.Request) (*hello.Response, error) {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnAdd in actor:%#v req:%#v\n", a.addr, req.String())
	return &hello.Response{R: req.A + req.B}, nil
}

func (s *HelloService) OnTestCallAdd(ctx *goactor.DispatchMessage, req *hello.Request) (*hello.Response, error) {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnCallSourceAdd in actor:%#v req:%#v\n", a.addr, req.String())

	rsp1, err1 := client.Add(system.Context(), system, a.actor, selector.Addr(), req, ctx.ExtractEx(goactor.HeaderIdTracingSpan))
	if err1 != nil {
		return nil, err1
	}
	rsp2, err2 := client.Add(system.Context(), system, a.actor, selector.Addr(), req, ctx.ExtractEx(goactor.HeaderIdTracingSpan))
	if err2 != nil {
		return nil, err2
	}
	return &hello.Response{R: rsp1.R + rsp2.R}, nil
}

type ClientActor struct {
}

func (c *ClientActor) OnInit(a goactor.Actor) {
	for i := int32(0); true; i++ {
		client.Send(system, a, selector.Addr(), &hello.Request{A: i, B: 10}, nil)
		rsp, err := client.TestCallAdd(system.Context(), system, a, selector.Addr(), &hello.Request{A: i, B: 10}, nil)
		log.Println("test call resource", rsp, err)
		a.Sleep(time.Second)
	}
}

func (c *ClientActor) OnRelease(a goactor.Actor) {
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
	system, err = goactor.NewActorSystem(
		goactor.WithName("tracing"),
		goactor.WithInstanceId(1),
		goactor.WithEtcd("http://10.21.248.213:2379"),
		goactor.WithContext(ctx),
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

	proto := protocols.NewProtobuf(1, goactor.ProtoWithInterceptorCall(func(msg *goactor.DispatchMessage, handler goactor.ProtoHandler, args ...interface{}) error {
		method := msg.Headers.GetStr(goactor.HeaderIdMethod)
		span := msg.Headers.GetInterface(goactor.HeaderIdTracingSpan)
		var spanContext opentracing.Span
		if span == nil {
			spanContext = tracer.StartSpan(method)
		} else {
			spanContext = tracer.StartSpan(method, opentracing.ChildOf(span.(opentracing.Span).Context()))
		}
		defer spanContext.Finish()

		carrier := new(bytes.Buffer)
		tracer.Inject(spanContext.Context(), opentracing.Binary, carrier)
		msg.Headers.Put(goactor.BuildHeaderBytes(goactor.HeaderIdTracingSpanCarrier, carrier.Bytes()))

		return handler(msg, args...)
	}), goactor.ProtoWithInterceptorDispatch(func(msg *goactor.DispatchMessage, handler goactor.ProtoHandler, args ...interface{}) error {
		spanCarrier := msg.Headers.GetBytes(goactor.HeaderIdTracingSpanCarrier)
		spanContext, err := tracer.Extract(opentracing.Binary, bytes.NewBuffer(spanCarrier))
		if err != nil {
			return goactor.ErrorWrap(err)
		}
		span := tracer.StartSpan("operation", opentracing.ChildOf(spanContext))
		defer span.Finish()
		msg.Headers.Put(goactor.BuildHeaderInterfaceRaw(goactor.HeaderIdTracingSpan, span, true))

		return handler(msg, args...)
	}))
	hello.RegisterHelloService(&HelloService{}, proto)
	client = hello.NewHelloServiceClient(proto)

	selector, err = goactor.NewRandomSelector(system, "hello")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		goactor.NewActor(&HelloActor{}, single, goactor.ActorWithProto(proto))
	}

	goactor.NewActor(&ClientActor{}, single, goactor.ActorWithProto(proto))

	select {
	case <-system.Context().Done():
	}
}
