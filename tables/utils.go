package tables

import (
	"context"

	"github.com/gocql/gocql"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// doWithTracing is a wrapper function that adds tracing, error handling and some other elements.
func doWithTracing(ctx context.Context, tracer trace.Tracer, spanName string, traceAttributes []attribute.KeyValue, doTracing bool, execute func(context.Context) error) error {
	tracedCtx := ctx
	var span trace.Span

	if doTracing {
		// Perform tracing
		tracedCtx, span = tracer.Start(ctx, spanName)
		span.SetAttributes(traceAttributes...)
		defer span.End()
	}

	// Execute the underlying operation
	err := execute(tracedCtx)

	// Handle not-founds as a silent nil return, but store errors into
	// our traces if we have one
	if err == gocql.ErrNotFound {
		return nil
	} else if err != nil {
		if doTracing {
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}

	return nil
}

// returnWithTracing is a wrapper function that adds tracing, error handling and some other elements.
func returnWithTracing[TResult any](ctx context.Context, tracer trace.Tracer, spanName string, traceAttributes []attribute.KeyValue, doTracing bool, execute func(context.Context) (TResult, error)) (TResult, error) {
	var dflt TResult
	tracedCtx := ctx
	var span trace.Span

	if doTracing {
		// Perform tracing
		tracedCtx, span = tracer.Start(ctx, spanName)
		span.SetAttributes(traceAttributes...)
		defer span.End()
	}

	// Execute the underlying operation
	result, err := execute(tracedCtx)

	// Handle not-founds as a silent nil return, but store errors into
	// our traces if we have one
	if err == gocql.ErrNotFound {
		return dflt, nil
	} else if err != nil {
		if doTracing {
			span.SetStatus(codes.Error, err.Error())
		}
		return dflt, err
	}

	return result, nil
}
