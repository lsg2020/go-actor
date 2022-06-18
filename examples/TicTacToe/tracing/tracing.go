package tracing

import (
	"bytes"

	goactor "github.com/lsg2020/go-actor"
	"github.com/opentracing/opentracing-go"
)

func InterceptorCall() goactor.ProtoOption {
	tracer := opentracing.GlobalTracer()
	return goactor.ProtoWithInterceptorCall(func(msg *goactor.DispatchMessage, handler goactor.ProtoHandler, args ...interface{}) error {
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
	})
}

func InterceptorDispatch() goactor.ProtoOption {
	tracer := opentracing.GlobalTracer()
	return goactor.ProtoWithInterceptorDispatch(func(msg *goactor.DispatchMessage, handler goactor.ProtoHandler, args ...interface{}) error {
		spanCarrier := msg.Headers.GetBytes(goactor.HeaderIdTracingSpanCarrier)
		spanContext, err := tracer.Extract(opentracing.Binary, bytes.NewBuffer(spanCarrier))
		if err != nil {
			return goactor.ErrorWrapf(err, "tracer extract err")
		}
		span := tracer.StartSpan("operation", opentracing.ChildOf(spanContext))
		defer span.Finish()
		msg.Headers.Put(goactor.BuildHeaderInterfaceRaw(goactor.HeaderIdTracingSpan, span, true))

		return handler(msg, args...)
	})
}
