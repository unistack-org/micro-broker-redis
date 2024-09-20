package redis

import (
	"time"

	"github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/meter"
	"go.unistack.org/micro/v3/tracer"
)

type configKey struct{}

func Config(c *redis.UniversalOptions) broker.Option {
	return broker.SetOption(configKey{}, c)
}

var (
	// DefaultSubscribeMaxInflight specifies how much messages keep inflight
	DefaultSubscribeMaxInflight = 100

	// DefaultMeterStatsInterval holds default stats interval
	DefaultMeterStatsInterval = 5 * time.Second
	// DefaultMeterMetricPrefix holds default metric prefix
	DefaultMeterMetricPrefix = "micro_broker_"
)

// Options struct holds wrapper options
type Options struct {
	Logger             logger.Logger
	Meter              meter.Meter
	Tracer             tracer.Tracer
	MeterMetricPrefix  string
	MeterStatsInterval time.Duration
}

// Option func signature
type Option func(*Options)

// NewOptions create new Options struct from provided option slice
func NewOptions(opts ...Option) Options {
	options := Options{
		Logger:             logger.DefaultLogger,
		Meter:              meter.DefaultMeter,
		Tracer:             tracer.DefaultTracer,
		MeterStatsInterval: DefaultMeterStatsInterval,
	}

	for _, o := range opts {
		o(&options)
	}

	options.Logger = options.Logger.Clone(logger.WithCallerSkipCount(1))

	return options
}

// MetricInterval specifies stats interval for *sql.DB
func MetricInterval(td time.Duration) Option {
	return func(o *Options) {
		o.MeterStatsInterval = td
	}
}

// MetricPrefix specifies prefix for each metric
func MetricPrefix(pref string) Option {
	return func(o *Options) {
		o.MeterMetricPrefix = pref
	}
}

type subscribeMaxInflightKey struct{}

// SubscribeMaxInFlight max queued messages
func SubscribeMaxInFlight(n int) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeMaxInflightKey{}, n)
}
