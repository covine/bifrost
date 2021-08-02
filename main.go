package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/exporters/stdout"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric/controller/push"
	"go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	sdkTrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/covine/bifrost/broker"
)

func main() {
	testOpenTelemetry()

	config, err := broker.Configure(os.Args[1:])
	if err != nil {
		log.Fatal("configure broker config error: ", err)
	}

	b, err := broker.NewBroker(config)
	if err != nil {
		log.Fatal("New broker error: ", err)
	}

	err = b.Start()
	if err != nil {
		log.Fatal("broker start error: ", err)
	}

	s := onSignal()
	log.Printf("signal received, broker closed: %v", s)

	b.Close()
}

func onSignal() os.Signal {
	sc := make(chan os.Signal, 1)
	defer close(sc)

	signal.Notify(sc, os.Kill, os.Interrupt)
	s := <-sc
	signal.Stop(sc)
	return s
}

var (
	fooKey     = label.Key("ex.com/foo")
	barKey     = label.Key("ex.com/bar")
	lemonsKey  = label.Key("ex.com/lemons")
	anotherKey = label.Key("ex.com/another")
)

func testOpenTelemetry() {
	exporter, err := stdout.NewExporter([]stdout.Option{
		stdout.WithQuantiles([]float64{0.5, 0.9, 0.99}),
		stdout.WithPrettyPrint(),
	}...)
	if err != nil {
		log.Fatalf("failed to initialize stdout export pipeline: %v", err)
	}

	bsp := sdkTrace.NewBatchSpanProcessor(exporter)
	tp := sdkTrace.NewTracerProvider(sdkTrace.WithSpanProcessor(bsp))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	pusher := push.New(
		basic.New(
			simple.NewWithExactDistribution(),
			exporter,
		),
		exporter,
	)
	pusher.Start()
	defer pusher.Stop()
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(pusher.MeterProvider())

	// set global propagator to baggage (the default is no-op).
	otel.SetTextMapPropagator(propagation.Baggage{})
	tracer := otel.Tracer("ex.com/basic")
	meter := otel.Meter("ex.com/basic")

	commonLabels := []label.KeyValue{lemonsKey.Int(10), label.String("A", "1"), label.String("B", "2"), label.String("C", "3")}

	oneMetricCB := func(_ context.Context, result metric.Float64ObserverResult) {
		result.Observe(1, commonLabels...)
	}
	_ = metric.Must(meter).NewFloat64ValueObserver("ex.com.one", oneMetricCB,
		metric.WithDescription("A ValueObserver set to 1.0"),
	)

	valuerecorderTwo := metric.Must(meter).NewFloat64ValueRecorder("ex.com.two")

	ctx := context.Background()
	ctx = baggage.ContextWithValues(ctx, fooKey.String("foo1"), barKey.String("bar1"))

	valuerecorder := valuerecorderTwo.Bind(commonLabels...)
	defer valuerecorder.Unbind()

	err = func(ctx context.Context) error {
		var span trace.Span
		ctx, span = tracer.Start(ctx, "operation")
		defer span.End()

		span.AddEvent("Nice operation!", trace.WithAttributes(label.Int("bogons", 100)))
		span.SetAttributes(anotherKey.String("yes"))

		meter.RecordBatch(
			// Note: call-site variables added as context Entries:
			baggage.ContextWithValues(ctx, anotherKey.String("xyz")),
			commonLabels,

			valuerecorderTwo.Measurement(2.0),
		)

		return func(ctx context.Context) error {
			var span trace.Span
			ctx, span = tracer.Start(ctx, "Sub operation...")
			defer span.End()

			span.SetAttributes(lemonsKey.String("five"))
			span.AddEvent("Sub span event")
			valuerecorder.Record(ctx, 1.3)

			return nil
		}(ctx)
	}(ctx)
	if err != nil {
		panic(err)
	}
}
