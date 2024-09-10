package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/codec"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/semconv"
)

var DefaultOptions = &redis.UniversalOptions{
	Username:        "",
	Password:        "", // no password set
	DB:              0,  // use default DB
	MaxRetries:      2,
	MaxRetryBackoff: 256 * time.Millisecond,
	DialTimeout:     1 * time.Second,
	ReadTimeout:     1 * time.Second,
	WriteTimeout:    1 * time.Second,
	PoolTimeout:     1 * time.Second,
	MinIdleConns:    10,
}

var (
	_ broker.Broker     = (*Broker)(nil)
	_ broker.Event      = (*Event)(nil)
	_ broker.Subscriber = (*Subscriber)(nil)
)

// Event is an broker.Event
type Event struct {
	ctx   context.Context
	err   error
	msg   *broker.Message
	topic string
}

// Topic returns the topic this Event applies to.
func (p *Event) Context() context.Context {
	return p.ctx
}

// Topic returns the topic this Event
func (p *Event) Topic() string {
	return p.topic
}

// Message returns the broker message
func (p *Event) Message() *broker.Message {
	return p.msg
}

// Ack sends an acknowledgement to the broker. However this is not supported
// is Redis and therefore this is a no-op.
func (p *Event) Ack() error {
	return nil
}

func (p *Event) Error() error {
	return p.err
}

func (p *Event) SetError(err error) {
	p.err = err
}

// Subscriber implements broker.Subscriber interface
type Subscriber struct {
	ctx    context.Context
	done   chan struct{}
	sub    *redis.PubSub
	topic  string
	handle broker.Handler
	opts   broker.Options
	sopts  broker.SubscribeOptions
}

// recv loops to receive new messages from Redis and handle them
// as Events.
func (s *Subscriber) loop() {
	maxInflight := DefaultSubscribeMaxInflight
	if s.opts.Context != nil {
		if n, ok := s.opts.Context.Value(subscribeMaxInflightKey{}).(int); n > 0 && ok {
			maxInflight = n
		}
	}

	eh := s.opts.ErrorHandler
	if s.sopts.ErrorHandler != nil {
		eh = s.sopts.ErrorHandler
	}

	for {
		select {
		case <-s.done:
			return
		case msg := <-s.sub.Channel(redis.WithChannelSize(maxInflight)):
			p := &Event{
				topic: msg.Channel,
				msg:   &broker.Message{},
			}

			err := s.opts.Codec.Unmarshal([]byte(msg.Payload), p.msg)
			p.ctx = metadata.NewIncomingContext(s.ctx, p.msg.Header)

			if err != nil {
				p.msg.Body = codec.RawMessage(msg.Payload)
				if eh != nil {
					_ = eh(p)
					continue
				}
				s.opts.Logger.Fatal(s.ctx, fmt.Sprintf("codec.Unmarshal error %v", err))
			}

			if p.err = s.handle(p); p.err != nil {
				if eh != nil {
					_ = eh(p)
					continue
				}
				s.opts.Logger.Fatal(s.ctx, fmt.Sprintf("handle error %v", err))

			}

			if s.sopts.AutoAck {
				if err := p.Ack(); err != nil {
					s.opts.Logger.Fatal(s.ctx, "auto ack error", err)
				}
			}
		}
	}
}

// Options returns the Subscriber options.
func (s *Subscriber) Options() broker.SubscribeOptions {
	return s.sopts
}

// Topic returns the topic of the Subscriber.
func (s *Subscriber) Topic() string {
	return s.topic
}

// Unsubscribe unsubscribes the Subscriber and frees the connection.
func (s *Subscriber) Unsubscribe(ctx context.Context) error {
	return s.sub.Unsubscribe(ctx, s.topic)
}

// Broker implements broker.Broker interface
type Broker struct {
	opts broker.Options
	cli  redis.UniversalClient
	done chan struct{}
}

// String returns the name of the broker implementation
func (b *Broker) String() string {
	return "redis"
}

// Name returns the name of the broker
func (b *Broker) Name() string {
	return b.opts.Name
}

// Options returns the broker.Broker Options
func (b *Broker) Options() broker.Options {
	return b.opts
}

// Address returns the address the broker will use to create new connections
func (b *Broker) Address() string {
	return strings.Join(b.opts.Addrs, ",")
}

func (b *Broker) BatchPublish(ctx context.Context, msgs []*broker.Message, opts ...broker.PublishOption) error {
	return b.publish(ctx, msgs, opts...)
}

func (b *Broker) Publish(ctx context.Context, topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	msg.Header.Set(metadata.HeaderTopic, topic)
	return b.publish(ctx, []*broker.Message{msg}, opts...)
}

func (b *Broker) publish(ctx context.Context, msgs []*broker.Message, opts ...broker.PublishOption) error {
	options := broker.NewPublishOptions(opts...)

	for _, msg := range msgs {
		var record string
		topic, _ := msg.Header.Get(metadata.HeaderTopic)
		msg.Header.Del(metadata.HeaderTopic)
		b.opts.Meter.Counter(semconv.PublishMessageInflight, "endpoint", topic, "topic", topic).Inc()
		if options.BodyOnly || b.opts.Codec.String() == "noop" {
			record = string(msg.Body)
		} else {
			buf, err := b.opts.Codec.Marshal(msg)
			if err != nil {
				return err
			}
			record = string(buf)
		}
		ts := time.Now()
		if err := b.cli.Publish(ctx, topic, record); err != nil {
			b.opts.Meter.Counter(semconv.PublishMessageTotal, "endpoint", topic, "topic", topic, "status", "failure").Inc()
		} else {
			b.opts.Meter.Counter(semconv.PublishMessageTotal, "endpoint", topic, "topic", topic, "status", "success").Inc()
		}
		te := time.Since(ts)
		b.opts.Meter.Summary(semconv.PublishMessageLatencyMicroseconds, "endpoint", topic, "topic", topic).Update(te.Seconds())
		b.opts.Meter.Histogram(semconv.PublishMessageDurationSeconds, "endpoint", topic, "topic", topic).Update(te.Seconds())
		b.opts.Meter.Counter(semconv.PublishMessageInflight, "endpoint", topic, "topic", topic).Dec()
	}
	return nil
}

// Subscribe returns a broker.BatchSubscriber for the topic and handler
func (b *Broker) BatchSubscribe(ctx context.Context, topic string, handler broker.BatchHandler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	return nil, nil
}

// Subscribe returns a broker.Subscriber for the topic and handler
func (b *Broker) Subscribe(ctx context.Context, topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	s := &Subscriber{
		ctx:    ctx,
		topic:  topic,
		handle: handler,
		opts:   b.opts,
		sopts:  broker.NewSubscribeOptions(opts...),
		done:   make(chan struct{}),
	}

	s.sub = b.cli.Subscribe(s.ctx, s.topic)
	if err := s.sub.Ping(ctx, ""); err != nil {
		return nil, err
	}

	go s.loop()

	return s, nil
}

func (b *Broker) configure() error {
	redisOptions := DefaultOptions

	if b.opts.Context != nil {
		if c, ok := b.opts.Context.Value(configKey{}).(*redis.UniversalOptions); ok && c != nil {
			redisOptions = c
		}
	}

	if len(b.opts.Addrs) > 0 {
		redisOptions.Addrs = b.opts.Addrs
	}

	if b.opts.TLSConfig != nil {
		redisOptions.TLSConfig = b.opts.TLSConfig
	}

	if redisOptions == nil && b.cli != nil {
		return nil
	}

	c := redis.NewUniversalClient(redisOptions)
	setTracing(c, b.opts.Tracer)

	b.cli = c
	b.statsMeter()

	return nil
}

func (b *Broker) Connect(ctx context.Context) error {
	var err error
	if b.cli != nil {
		err = b.cli.Ping(ctx).Err()
	}
	setSpanError(ctx, err)
	b.done = make(chan struct{})
	return err
}

func (b *Broker) Init(opts ...broker.Option) error {
	for _, o := range opts {
		o(&b.opts)
	}

	err := b.configure()
	if err != nil {
		return err
	}

	return nil
}

func (b *Broker) Client() redis.UniversalClient {
	if b.cli != nil {
		return b.cli
	}
	return nil
}

func (b *Broker) Disconnect(ctx context.Context) error {
	var err error
	select {
	case <-b.done:
		return err
	default:
		if b.cli != nil {
			err = b.cli.Close()
		}
		close(b.done)
		return err
	}
}

func NewBroker(opts ...broker.Option) *Broker {
	return &Broker{opts: broker.NewOptions(opts...)}
}
