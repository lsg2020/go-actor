package tracing

import (
	"bytes"
	"context"
	"io"
	"time"

	goactor "github.com/lsg2020/go-actor"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

const ContextTagsKey = "tracing_tags"

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

func InterceptorCallTags(tracer opentracing.Tracer, f func(msg *goactor.DispatchMessage) opentracing.Tags) goactor.ProtoOption {
	return goactor.ProtoWithInterceptorCall(func(msg *goactor.DispatchMessage, handler goactor.ProtoHandler, args ...interface{}) error {
		tags := f(msg)
		if tags != nil {
			ctx := msg.Context()
			ctx = context.WithValue(ctx, ContextTagsKey, tags)
			msg.SetContext(ctx)
		}
		return handler(msg, args...)
	})
}

func InterceptorCall(tracer opentracing.Tracer) goactor.ProtoOption {
	return goactor.ProtoWithInterceptorCall(func(msg *goactor.DispatchMessage, handler goactor.ProtoHandler, args ...interface{}) error {
		method := msg.Headers.GetStr(goactor.HeaderIdMethod)
		ctx := msg.Context()
		tags := ctx.Value(ContextTagsKey)
		span := opentracing.SpanFromContext(ctx)
		var spanContext opentracing.Span
		if span == nil {
			if tags != nil {
				spanContext = tracer.StartSpan(method, tags.(opentracing.Tags))
			} else {
				spanContext = tracer.StartSpan(method)
			}
		} else {
			if tags != nil {
				spanContext = tracer.StartSpan(method, opentracing.ChildOf(span.(opentracing.Span).Context()), tags.(opentracing.Tags))
			} else {
				spanContext = tracer.StartSpan(method, opentracing.ChildOf(span.(opentracing.Span).Context()))
			}
		}
		defer spanContext.Finish()

		carrier := new(bytes.Buffer)
		tracer.Inject(spanContext.Context(), opentracing.Binary, carrier)
		msg.Headers.Put(goactor.BuildHeaderBytes(goactor.HeaderIdTracingSpanCarrier, carrier.Bytes()))

		return handler(msg, args...)
	})
}

func InterceptorDispatchTags(tracer opentracing.Tracer, f func(msg *goactor.DispatchMessage) opentracing.Tags) goactor.ProtoOption {
	return goactor.ProtoWithInterceptorDispatch(func(msg *goactor.DispatchMessage, handler goactor.ProtoHandler, args ...interface{}) error {
		tags := f(msg)
		if tags != nil {
			ctx := msg.Context()
			ctx = context.WithValue(ctx, ContextTagsKey, tags)
			msg.SetContext(ctx)
		}
		return handler(msg, args...)
	})
}

func InterceptorDispatch(tracer opentracing.Tracer) goactor.ProtoOption {
	return goactor.ProtoWithInterceptorDispatch(func(msg *goactor.DispatchMessage, handler goactor.ProtoHandler, args ...interface{}) error {
		spanCarrier := msg.Headers.GetBytes(goactor.HeaderIdTracingSpanCarrier)
		spanContext, err := tracer.Extract(opentracing.Binary, bytes.NewBuffer(spanCarrier))
		if err != nil {
			return goactor.ErrorWrapf(err, "tracer extract err")
		}
		var span opentracing.Span
		ctx := msg.Context()
		tags := ctx.Value(ContextTagsKey)
		if tags != nil {
			span = tracer.StartSpan("operation", opentracing.ChildOf(spanContext), tags.(opentracing.Tags))
		} else {
			span = tracer.StartSpan("operation", opentracing.ChildOf(spanContext))
		}
		defer span.Finish()

		ctx = opentracing.ContextWithSpan(ctx, span)
		msg.SetContext(ctx)

		return handler(msg, args...)
	})
}
