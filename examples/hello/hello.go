package main

import (
	"context"
	"fmt"
	"time"

	goactor "github.com/lsg2020/go-actor"
	"github.com/lsg2020/go-actor/executer"
	"go.uber.org/zap"
)

type Actor struct {
	Id     int
	Amount int
}

func (a *Actor) OnInit(actor goactor.Actor) {
	actor.Fork(func() {
		for {
			a.Amount++
			actor.Logger().Info(fmt.Sprintf("%#v test sleep", a))
			actor.Sleep(time.Second)
		}
	})

	actor.Timeout(time.Second*3, func() {
		if a.Id != 0 {
			actor.Kill()
		}
	})

	execRet, execErr := actor.Exec(actor.Context(), func() (interface{}, error) {
		panic("test err")
		return nil, nil
	})
	_ = execRet
	actor.Logger().Info("exec response", zap.Error(execErr))

	actor.Logger().Info(fmt.Sprintf("actor init %#v", a))
}

func (a *Actor) OnRelease(actor goactor.Actor) {
	actor.Logger().Info(fmt.Sprintf("actor release %#v", a))
}

func main() {
	single := &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)

	for i := 0; i < 2; i++ {
		goactor.NewActor(&Actor{
			Id: i,
		}, single)
	}

	<-context.Background().Done()
}
