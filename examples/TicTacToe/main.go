package main

import (
	"context"
	"embed"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	goactor "github.com/lsg2020/go-actor"
	"github.com/lsg2020/go-actor/examples/TicTacToe/controller"
	"github.com/lsg2020/go-actor/examples/TicTacToe/game"
	message "github.com/lsg2020/go-actor/examples/TicTacToe/pb"
	"github.com/lsg2020/go-actor/executer"
	"github.com/lsg2020/go-actor/protocols"
)

//go:embed html
var Html embed.FS

func main() {
	system, err := goactor.NewActorSystem(goactor.WithName("game"), goactor.WithInstanceId(1), goactor.WithEtcd("10.21.248.213:2379"))
	if err != nil {
		log.Panicln(err)
	}

	single := &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)

	manager := &game.Manager{System: system}
	proto := protocols.NewProtobuf(1)
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
