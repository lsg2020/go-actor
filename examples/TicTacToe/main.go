package main

import (
	"context"
	"embed"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	goactor "github.com/lsg2020/go-actor"
	"github.com/lsg2020/go-actor/examples/TicTacToe/controller"
	"github.com/lsg2020/go-actor/examples/TicTacToe/game"
	message "github.com/lsg2020/go-actor/examples/TicTacToe/pb"
	"github.com/lsg2020/go-actor/examples/TicTacToe/tracing"
	"github.com/lsg2020/go-actor/executer"
	"github.com/lsg2020/go-actor/protocols"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

//go:embed html
var Html embed.FS

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
	system, err := goactor.NewActorSystem(goactor.WithName("game"), goactor.WithInstanceId(1), goactor.WithEtcd("10.21.248.213:2379"))
	if err != nil {
		log.Panicln(err)
	}

	tracer, _, err := NewTracer("hello_2", "10.21.248.28:5775")
	if err != nil {
		panic(err)
	}
	opentracing.SetGlobalTracer(tracer)

	single := &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)

	manager := &game.Manager{System: system}
	proto := protocols.NewProtobuf(1, tracing.InterceptorCall(), tracing.InterceptorDispatch())
	message.RegisterManagerService(manager, proto)
	managerActor := goactor.NewActor(manager, single, goactor.ActorWithProto(proto))
	system.Register(managerActor, "manager")

	router := mux.NewRouter()
	router.Handle("/", controller.StaticFileRouter{FS: &Html, Path: "html/index.html"})
	router.PathPrefix("/html/").Handler(controller.StaticFileRouter{FS: &Html, StripPrefix: "/"})
	router.PathPrefix("/Game/").Handler(http.StripPrefix("/Game", controller.NewGameController(system)))
	router.PathPrefix("/Home/").Handler(http.StripPrefix("/Home", controller.NewHomeController()))

	server := http.Server{Handler: router, Addr: "0.0.0.0:80"}
	server.ListenAndServe()
}
