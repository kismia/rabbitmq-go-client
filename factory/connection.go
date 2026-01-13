package factory

import (
	"crypto/tls"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/kismia/rabbitmq-go-client"
	"github.com/kismia/rabbitmq-go-client/balancer"
	"github.com/kismia/rabbitmq-go-client/recovery"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultHeartbeat = 10 * time.Second
	defaultLocale    = "en_US"
)

type BackOffFactory func() backoff.BackOff

var defaultBackOffFactory BackOffFactory = func() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = time.Millisecond
	b.MaxInterval = time.Second
	b.MaxElapsedTime = time.Second * 5

	return b
}

type ConnectionFactory struct {
	Hosts             []string
	Username          string
	Password          string
	Vhost             string
	ChannelMax        int
	FrameSize         int
	Heartbeat         time.Duration
	TLSClientConfig   *tls.Config
	Properties        amqp.Table
	Locale            string
	ConnectionTimeout time.Duration
	Balancer          rabbitmq.Balancer
	BackOffFactory    BackOffFactory
	AutomaticRecovery bool
	TopologyRecovery  bool
	ErrorHandler      rabbitmq.ErrorHandler
}

func (f *ConnectionFactory) NewConnection() (rabbitmq.Connection, error) {
	if f.Heartbeat == 0 {
		f.Heartbeat = defaultHeartbeat
	}

	if len(f.Locale) == 0 {
		f.Locale = defaultLocale
	}

	config := f.config()

	if f.Balancer == nil {
		f.Balancer = balancer.NewRoundRobinBalancer(f.Hosts)
	}

	if f.ConnectionTimeout > 0 {
		config.Dial = amqp.DefaultDial(f.ConnectionTimeout)
	}

	if !f.AutomaticRecovery {
		conn, err := rabbitmq.NewConnection(config, f.Balancer)
		if err != nil {
			return nil, err
		}

		return &rabbitmq.ConnectionAdapter{Connection: conn}, nil
	}

	if f.ErrorHandler == nil {
		f.ErrorHandler = recovery.ForgivingErrorHandler{}
	}

	if f.BackOffFactory == nil {
		f.BackOffFactory = defaultBackOffFactory
	}

	return recovery.NewConnection(config, f.Balancer, f.BackOffFactory(), f.ErrorHandler, f.TopologyRecovery)
}

func (f *ConnectionFactory) config() amqp.Config {
	return amqp.Config{
		SASL: []amqp.Authentication{
			&amqp.PlainAuth{
				Username: f.Username,
				Password: f.Password,
			},
		},
		Vhost:           f.Vhost,
		ChannelMax:      uint16(f.ChannelMax),
		FrameSize:       f.FrameSize,
		Heartbeat:       f.Heartbeat,
		TLSClientConfig: f.TLSClientConfig,
		Properties:      f.Properties,
		Locale:          f.Locale,
	}
}
