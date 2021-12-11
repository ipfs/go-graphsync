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

// Collector can be used as a trace batcher to provide traces to, we collect
// individual spans and then extract useful data out of them for test assertions
type Collector struct {
	Spans tracetest.SpanStubs
}

// ExportSpans receives the ReadOnlySpans from the batch provider
func (c *Collector) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	c.Spans = tracetest.SpanStubsFromReadOnlySpans(spans)
	return nil
}

// Shutdown is a noop, we don't need to do anything fancy
func (c *Collector) Shutdown(ctx context.Context) error {
	return nil
}

// FindSpans returns a list of spans by their name
func (c Collector) FindSpans(name string) tracetest.SpanStubs {
	var found = tracetest.SpanStubs{}
	for _, s := range c.Spans {
		if s.Name == name {
			found = append(found, s)
		}
	}
	return found
}

// TracesToString returns an array of all traces represented as strings with each
// span in the trace identified by name and its number (within the parent span)
// in parens, separated by a '->'. e.g. `"foo(0)->bar(0)","foo(0)->bar(1)"`
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

// FindSpanByTraceString is similar to FindSpans but returns a single span
// identified by its trace string as described in TracesToStrings. Note that
// this string can also be a partial of a complete trace, e.g. just `"foo(0)"`
// without any children to fetch the parent span.
func (c Collector) FindSpanByTraceString(trace string) *tracetest.SpanStub {
	var found *tracetest.SpanStub
	c.tracesToString("", c.FindParentSpans(), trace, func(span tracetest.SpanStub) {
		if found != nil && found.Name != "" {
			panic("found more than one span with the same trace string")
		}
		found = &span
	})
	return found
}

// FindParentSpans finds spans that have no parents, they are at the top any
// stack.
func (c Collector) FindParentSpans() tracetest.SpanStubs {
	var found = tracetest.SpanStubs{}
	for _, s := range c.Spans {
		if s.Parent.SpanID() == [8]byte{} {
			found = append(found, s)
		}
	}
	return found
}

// FindSpansWithParent finds spans that are children of the provided span.
func (c Collector) FindSpansWithParent(stub tracetest.SpanStub) tracetest.SpanStubs {
	var found = tracetest.SpanStubs{}
	for _, s := range c.Spans {
		if s.Parent.SpanID() == stub.SpanContext.SpanID() {
			found = append(found, s)
		}
	}
	return found
}

// SingleExceptionEvent is a test helper that asserts that a span, identified by a
// trace string (see TracesToStrings) contains a single exception, identified by
// the type (regexp) and message (regexp). If errorCode is true, then we also assert
// that the span has an error status code, with the same message (regexp)
func (c Collector) SingleExceptionEvent(t *testing.T, trace string, typeRe string, messageRe string, errorCode bool) {
	t.Helper()

	// has ContextCancelError exception recorded in the right place
	et := c.FindSpanByTraceString(trace)
	require.Len(t, et.Events, 1, "expected one event in span %v", trace)
	ex := EventAsException(t, EventInTraceSpan(t, *et, "exception"))
	require.Regexp(t, typeRe, ex.Type)
	require.Regexp(t, messageRe, ex.Message)
	if errorCode {
		require.Equal(t, codes.Error, et.Status.Code)
		require.Regexp(t, messageRe, et.Status.Description)
	}
}

// SetupTracing returns a test helper that can will collect all spans within
// a Collector. The returned helper function should be called at the point in
// a test where the spans are ready to be analyzed. Any spans not properly
// completed at that point won't be represented in the Collector.
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

// AttributeValueInTraceSpan is a test helper that asserts that at a span
// contains an attribute with the name provided, and returns the value of
// that attribute for further inspection.
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

// EventInTraceSpan is a test helper that asserts that at a span
// contains an event with the name provided, and returns the value of
// that event for further inspection.
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

// ExceptionEvent is a simplistic string form representation of an event
type ExceptionEvent struct {
	Type    string
	Message string
}

// EventAsException is a test helper that converts a trace event to an ExceptionEvent
// for easier inspection.
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
