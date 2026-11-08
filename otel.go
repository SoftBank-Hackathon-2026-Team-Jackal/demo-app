package main

// OpenTelemetry 계측. OTEL_EXPORTER_OTLP_ENDPOINT가 비어 있으면 아무것도 내보내지 않는다(no-op).
//
// 트래픽 전환 판정은 중계 서버 Envoy의 카운터로 하고, 여기서 내보내는 데이터는 앱 안쪽을 보여 주는 용도다:
// 엔드포인트별 지연·상태 코드, DB 쿼리 시간과 어느 DB 주소(chat-db / PgBouncer)로 갔는지, 커넥션 풀 대기.
//
// 코드가 직접 읽는 설정은 OTEL_EXPORTER_OTLP_ENDPOINT 하나다. 나머지(OTEL_EXPORTER_OTLP_HEADERS,
// OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES 등)는 SDK가 표준 환경 변수로 읽는다.
// 메트릭은 5초마다 보낸다(이전 단계 화면이 몇 초 단위로 바뀌므로 SDK 기본 60초는 너무 길다).

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "demo-app"

var (
	attrAppEnv    = attribute.Key("app.env")
	attrHTTPRoute = attribute.Key("http.route")
)

// setupOTel은 전역 TracerProvider·MeterProvider를 설정하고 종료 함수를 돌려준다.
func setupOTel(ctx context.Context, envName string) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return noop, nil
	}
	// service.name 기본값은 chat-app이고 OTEL_SERVICE_NAME·OTEL_RESOURCE_ATTRIBUTES가 있으면 그 값이 이긴다
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", "chat-app"),
		attribute.String("deployment.environment.name", envName),
		attrAppEnv.String(envName),
	))
	if err != nil {
		return noop, err
	}
	if extra, err := resource.New(ctx, resource.WithFromEnv(), resource.WithHost()); err == nil {
		res, _ = resource.Merge(res, extra)
	}

	var shutdowns []func(context.Context) error
	shutdown := func(ctx context.Context) error {
		var errs []error
		for _, fn := range shutdowns {
			errs = append(errs, fn(ctx))
		}
		return errors.Join(errs...)
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	{
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return shutdown, err
		}
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithBatcher(exp),
			sdktrace.WithSampler(sdktrace.ParentBased(dropOrphanClientSpans{sdktrace.AlwaysSample()})),
		)
		otel.SetTracerProvider(tp)
		shutdowns = append(shutdowns, tp.Shutdown)
	}
	{
		exp, err := otlpmetrichttp.New(ctx)
		if err != nil {
			return shutdown, err
		}
		reader := sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(5*time.Second))
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader))
		otel.SetMeterProvider(mp)
		shutdowns = append(shutdowns, mp.Shutdown)
	}
	log.Printf("otel: exporting to %s", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	return shutdown, nil
}

// dropOrphanClientSpans는 부모 없는 DB 클라이언트 span(hub가 150ms마다 하는 폴링)을 버린다.
// 요청 처리 중에 생긴 DB span은 부모가 있으므로 ParentBased 쪽에서 그대로 기록된다.
type dropOrphanClientSpans struct{ next sdktrace.Sampler }

func (s dropOrphanClientSpans) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if p.Kind == trace.SpanKindClient {
		return sdktrace.SamplingResult{Decision: sdktrace.Drop, Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState()}
	}
	return s.next.ShouldSample(p)
}

func (s dropOrphanClientSpans) Description() string { return "DropOrphanClientSpans" }

// instrumentHTTP는 서버 span과 http.server.* 메트릭을 남긴다. mux에서 찾은 라우트 패턴을 span 이름과
// http.route 속성(메트릭 포함)으로 쓴다. SSE(/api/stream)는 연결이 몇 분씩 유지되어 지연 분포를 흐리고,
// /healthz는 프로브라서 제외한다.
func instrumentHTTP(mux *http.ServeMux, next http.Handler) http.Handler {
	routeOf := func(r *http.Request) string {
		_, pattern := mux.Handler(r)
		return pattern
	}
	routed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if pattern := routeOf(r); pattern != "" {
			attr := attrHTTPRoute.String(pattern)
			trace.SpanFromContext(r.Context()).SetAttributes(attr)
			if l, ok := otelhttp.LabelerFromContext(r.Context()); ok {
				l.Add(attr)
			}
		}
		next.ServeHTTP(w, r)
	})
	return otelhttp.NewHandler(routed, "http",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			if pattern := routeOf(r); pattern != "" {
				return pattern
			}
			return r.Method
		}),
		otelhttp.WithFilter(func(r *http.Request) bool {
			return r.URL.Path != "/healthz" && r.URL.Path != "/api/stream"
		}),
	)
}

type appMetrics struct {
	created metric.Int64Counter
}

// newAppMetrics는 앱 고유 메트릭을 등록한다. MeterProvider가 no-op이면 기록도 no-op이다.
func newAppMetrics(pool *pgxpool.Pool, envName string) *appMetrics {
	meter := otel.Meter(instrumentationName)
	created, _ := meter.Int64Counter("chat.messages.created",
		metric.WithDescription("새로 저장된 메시지 수(같은 client_msg_id 재전송은 제외)"))

	// 커넥션 풀: DB 전환 중 PgBouncer PAUSE 동안 acquired가 차고 대기가 늘어나는 모습이 보인다
	acquired, _ := meter.Int64ObservableGauge("db.client.pool.acquired", metric.WithDescription("사용 중인 연결 수"))
	idle, _ := meter.Int64ObservableGauge("db.client.pool.idle", metric.WithDescription("유휴 연결 수"))
	waits, _ := meter.Int64ObservableCounter("db.client.pool.empty_acquires", metric.WithDescription("빈 풀에서 기다린 획득 수(누적)"))
	waitTime, _ := meter.Float64ObservableCounter("db.client.pool.acquire_wait", metric.WithUnit("s"),
		metric.WithDescription("빈 풀에서 기다린 시간(누적)"))
	envAttr := metric.WithAttributes(attrAppEnv.String(envName))
	_, _ = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		st := pool.Stat()
		o.ObserveInt64(acquired, int64(st.AcquiredConns()), envAttr)
		o.ObserveInt64(idle, int64(st.IdleConns()), envAttr)
		o.ObserveInt64(waits, st.EmptyAcquireCount(), envAttr)
		o.ObserveFloat64(waitTime, st.EmptyAcquireWaitTime().Seconds(), envAttr)
		return nil
	}, acquired, idle, waits, waitTime)

	return &appMetrics{created: created}
}
