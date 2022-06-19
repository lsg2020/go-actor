package recovery

import (
	goactor "github.com/lsg2020/go-actor"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

func InterceptorDispatch(tracer opentracing.Tracer) goactor.ProtoOption {
	return goactor.ProtoWithInterceptorDispatch(func(msg *goactor.DispatchMessage, handler goactor.ProtoHandler, args ...interface{}) error {
		defer func() {
			actor := msg.Actor
			if actor == nil || actor.GetState() == goactor.ActorStateStop {
				return
			}

			if r := recover(); r != nil {
				actor.Logger().Error("dispatch panic", zap.String("method", msg.Headers.GetStr(goactor.HeaderIdMethod)), zap.Any("panic", r))
			}
		}()
		return handler(msg, args...)
	})
}
