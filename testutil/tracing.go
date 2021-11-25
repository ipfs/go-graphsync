package testutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var _ trace.SpanExporter = &Collector{}

type Collector struct {
	Spans tracetest.SpanStubs
}

func (c *Collector) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	c.Spans = tracetest.SpanStubsFromReadOnlySpans(spans)
	return nil
}

func (c *Collector) Shutdown(ctx context.Context) error {
	return nil
}

func (c Collector) FindSpans(name string) tracetest.SpanStubs {
	var found = tracetest.SpanStubs{}
	for _, s := range c.Spans {
		if s.Name == name {
			found = append(found, s)
		}
	}
	return found
}

// TracesToString returns an array of traces represented as strings with each
// span in the trace identified by name separated by a '->'
func (c Collector) TracesToStrings() []string {
	return c.tracesToString("", c.FindParentSpans(), "", func(_ tracetest.SpanStub) {})
}

func (c Collector) tracesToString(trace string, spans tracetest.SpanStubs, matchString string, matchCb func(tracetest.SpanStub)) []string {
	var traces []string
	counts := make(map[string]int) // count the span children by name
	for _, span := range spans {
		nc := counts[span.Name]
		counts[span.Name] = nc + 1
		t := fmt.Sprintf("%v(%d)", span.Name, nc)
		if trace != "" {
			t = fmt.Sprintf("%v->%v", trace, t)
		}
		if t == matchString {
			matchCb(span)
		}
		children := c.FindSpansWithParent(span)
		if len(children) > 0 {
			traces = append(traces, c.tracesToString(t, children, matchString, matchCb)...)
		} else {
			traces = append(traces, t)
		}
	}
	return traces
}

func (c Collector) FindSpanByTraceString(trace string) tracetest.SpanStubs {
	var found = tracetest.SpanStubs{}
	c.tracesToString("", c.FindParentSpans(), trace, func(span tracetest.SpanStub) {
		found = append(found, span)
	})
	return found
}

func (c Collector) FindParentSpans() tracetest.SpanStubs {
	var found = tracetest.SpanStubs{}
	for _, s := range c.Spans {
		if s.Parent.SpanID() == [8]byte{} {
			found = append(found, s)
		}
	}
	return found
}

func (c Collector) FindSpansWithParent(stub tracetest.SpanStub) tracetest.SpanStubs {
	var found = tracetest.SpanStubs{}
	for _, s := range c.Spans {
		if s.Parent.SpanID() == stub.SpanContext.SpanID() {
			found = append(found, s)
		}
	}
	return found
}

func (c Collector) SingleExceptionEvent(t *testing.T, trace string, typeRe string, messageRe string, errorCode bool) {
	t.Helper()

	// has ContextCancelError exception recorded in the right place
	et := c.FindSpanByTraceString(trace)
	require.Len(t, et, 1, "expected one span with trace %v", trace)
	require.Len(t, et[0].Events, 1, "expected one event in span %v", trace)
	ex := EventAsException(t, EventInTraceSpan(t, et[0], "exception"))
	require.Regexp(t, typeRe, ex.Type)
	require.Regexp(t, messageRe, ex.Message)
	if errorCode {
		require.Equal(t, codes.Error, et[0].Status.Code)
		require.Regexp(t, messageRe, et[0].Status.Description)
	}
}

func SetupTracing() func(t *testing.T) *Collector {
	collector := &Collector{}
	tp := trace.NewTracerProvider(trace.WithBatcher(collector))
	otel.SetTracerProvider(tp)

	collect := func(t *testing.T) *Collector {
		t.Helper()

		require.NoError(t, tp.Shutdown(context.Background()))
		return collector
	}

	return collect
}

func AttributeValueInTraceSpan(t *testing.T, stub tracetest.SpanStub, attributeName string) attribute.Value {
	t.Helper()

	for _, attr := range stub.Attributes {
		if attr.Key == attribute.Key(attributeName) {
			return attr.Value
		}
	}
	require.Fail(t, "did not find expected attribute %v on trace span %v", attributeName, stub.Name)
	return attribute.Value{}
}

func EventInTraceSpan(t *testing.T, stub tracetest.SpanStub, eventName string) trace.Event {
	t.Helper()

	for _, evt := range stub.Events {
		if evt.Name == eventName {
			return evt
		}
	}
	require.Fail(t, "did not find expected event %v on trace span %v", eventName, stub.Name)
	return trace.Event{}
}

type ExceptionEvent struct {
	Type    string
	Message string
}

func EventAsException(t *testing.T, evt trace.Event) ExceptionEvent {
	t.Helper()

	var typ string
	var msg string
	for _, attr := range evt.Attributes {
		if attr.Key == attribute.Key("exception.type") {
			typ = attr.Value.AsString()
		} else if attr.Key == attribute.Key("exception.message") {
			msg = attr.Value.AsString()
		}
	}
	require.NotEmpty(t, typ, "expected non-empty exception.type attribute for %v", evt.Name)
	require.NotEmpty(t, msg, "expected non-empty exception.message attribute for %v", evt.Name)
	return ExceptionEvent{Type: typ, Message: msg}
}
