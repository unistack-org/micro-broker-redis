package redis

import (
	"context"
	"fmt"
	"net"
	"strconv"

	rediscmd "github.com/redis/go-redis/extra/rediscmd/v9"
	"github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/tracer"
)

func setTracing(rdb redis.UniversalClient, tr tracer.Tracer, opts ...tracer.SpanOption) {
	switch rdb := rdb.(type) {
	case *redis.Client:
		opt := rdb.Options()
		connString := formatDBConnString(opt.Network, opt.Addr)
		rdb.AddHook(newTracingHook(connString, tr))
	case *redis.ClusterClient:
		rdb.OnNewNode(func(rdb *redis.Client) {
			opt := rdb.Options()
			connString := formatDBConnString(opt.Network, opt.Addr)
			rdb.AddHook(newTracingHook(connString, tr))
		})
	case *redis.Ring:
		rdb.OnNewNode(func(rdb *redis.Client) {
			opt := rdb.Options()
			connString := formatDBConnString(opt.Network, opt.Addr)
			rdb.AddHook(newTracingHook(connString, tr))
		})
	}
}

type tracingHook struct {
	tr   tracer.Tracer
	opts []tracer.SpanOption
}

var _ redis.Hook = (*tracingHook)(nil)

func newTracingHook(connString string, tr tracer.Tracer, opts ...tracer.SpanOption) *tracingHook {
	opts = append(opts, tracer.WithSpanKind(tracer.SpanKindClient))
	if connString != "" {
		opts = append(opts, tracer.WithSpanLabels("db.connection_string", connString))
	}

	return &tracingHook{
		tr:   tr,
		opts: opts,
	}
}

func (h *tracingHook) DialHook(hook redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		/*
			_, span := h.tr.Start(ctx, "redis.dial", h.opts...)
			defer span.Finish()
		*/
		conn, err := hook(ctx, network, addr)
		// recordError(span, err)

		return conn, err
	}
}

func (h *tracingHook) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		cmdString := rediscmd.CmdString(cmd)
		var err error

		switch cmdString {
		case "cluster slots":
			break
		default:
			_, span := h.tr.Start(ctx, "redis.process", append(h.opts, tracer.WithSpanLabels("db.statement", cmdString))...)
			defer func() {
				recordError(span, err)
				span.Finish()
			}()
		}

		err = hook(ctx, cmd)

		return err
	}
}

func (h *tracingHook) ProcessPipelineHook(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		_, cmdsString := rediscmd.CmdsString(cmds)

		opts := append(h.opts, tracer.WithSpanLabels(
			"db.redis.num_cmd", strconv.Itoa(len(cmds)),
			"db.statement", cmdsString,
		))

		_, span := h.tr.Start(ctx, "redis.process_pipeline", opts...)
		defer span.Finish()

		err := hook(ctx, cmds)
		recordError(span, err)

		return err
	}
}

func setSpanError(ctx context.Context, err error) {
	if err == nil || err == redis.Nil {
		return
	}
	if sp, ok := tracer.SpanFromContext(ctx); !ok && sp != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
	}
}

func recordError(span tracer.Span, err error) {
	if err != nil && err != redis.Nil {
		span.SetStatus(tracer.SpanStatusError, err.Error())
	}
}

func formatDBConnString(network, addr string) string {
	if network == "tcp" {
		network = "redis"
	}
	return fmt.Sprintf("%s://%s", network, addr)
}
