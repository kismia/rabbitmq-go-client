package recovery

import (
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/kismia/rabbitmq-go-client"
	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrRecoverBackOff = &amqp.Error{Code: amqp.ChannelError, Reason: "channel/connection recovery back-off"}

type connection struct {
	config           amqp.Config
	topologyRecovery bool
	balancer         rabbitmq.Balancer
	backoff          backoff.BackOff
	errorHandler     rabbitmq.ErrorHandler
	delegate         *atomicConnection
	channelsMx       sync.RWMutex
	channels         map[*channel]struct{}
	topologyMx       sync.RWMutex
	exchanges        map[string]*exchange
	exchangeBindings map[string]*exchangeBinding
	queues           map[string]*queue
	queueBindings    map[string]*queueBinding
	consumers        map[string]*consumer
	notifyMx         sync.RWMutex
	recoveries       []chan *amqp.Connection
	blocks           []chan amqp.Blocking
	closes           []chan *amqp.Error
	destructor       sync.Once
}

func NewConnection(
	config amqp.Config,
	balancer rabbitmq.Balancer,
	backoff backoff.BackOff,
	errHandler rabbitmq.ErrorHandler,
	topologyRecovery bool,
) (*connection, error) {
	c := &connection{
		config:           config,
		balancer:         balancer,
		backoff:          backoff,
		errorHandler:     errHandler,
		channels:         make(map[*channel]struct{}),
		exchanges:        make(map[string]*exchange),
		exchangeBindings: make(map[string]*exchangeBinding),
		queues:           make(map[string]*queue),
		queueBindings:    make(map[string]*queueBinding),
		consumers:        make(map[string]*consumer),
		topologyRecovery: topologyRecovery,
	}

	conn, err := rabbitmq.NewConnection(c.config, c.balancer)
	if err != nil {
		return nil, err
	}

	c.delegate = newAtomicConnection(conn)

	go c.addAutomaticRecoveryListener(conn)

	return c, nil
}

func (c *connection) Channel() (rabbitmq.Channel, error) {
	ch, err := c.delegate.load().Channel()
	if err != nil {
		return nil, err
	}

	return c.wrapChannel(ch), nil
}

func (c *connection) NotifyBlocked(receiver chan amqp.Blocking) chan amqp.Blocking {
	c.notifyMx.Lock()
	c.blocks = append(c.blocks, receiver)
	c.notifyMx.Unlock()

	go blockingListener(c.delegate.load(), receiver)

	return receiver
}

func (c *connection) NotifyRecovery(receiver chan *amqp.Connection) chan *amqp.Connection {
	c.notifyMx.Lock()
	c.recoveries = append(c.recoveries, receiver)
	c.notifyMx.Unlock()

	return receiver
}

func (c *connection) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	c.notifyMx.Lock()
	c.closes = append(c.closes, receiver)
	c.notifyMx.Unlock()

	return receiver
}

func (c *connection) wrapChannel(ch *amqp.Channel) *channel {
	wrappedCh := newChannel(c, ch)

	c.registerChannel(wrappedCh)

	return wrappedCh
}

func (c *connection) registerChannel(ch *channel) {
	c.channelsMx.Lock()
	c.channels[ch] = struct{}{}
	c.channelsMx.Unlock()
}

func (c *connection) unregisterChannel(ch *channel) {
	c.channelsMx.Lock()
	delete(c.channels, ch)
	c.channelsMx.Unlock()
}

func (c *connection) addAutomaticRecoveryListener(conn *amqp.Connection) {
	rabbitmq.Logger.Print("start connection recovery listener")

	if err, ok := <-conn.NotifyClose(make(chan *amqp.Error)); ok {
		if shouldTriggerConnectionRecovery(err) {
			rabbitmq.Logger.Println("error connection closed:", err)
			c.beginAutomaticRecovery()
		}
	}
}

func shouldTriggerConnectionRecovery(err *amqp.Error) bool {
	if err != nil {
		switch err.Code {
		case amqp.ChannelError, amqp.ConnectionForced:
			return true
		}
	}

	return false
}

func (c *connection) beginAutomaticRecovery() {
	conn, err := c.recoverConnection()
	if err != nil {
		c.shutdown(err)
		c.errorHandler.HandleConnectionRecoveryError(c, err)
		return
	}

	go c.addAutomaticRecoveryListener(conn)

	c.recoverChannels(conn)

	c.delegate.store(conn)

	if c.topologyRecovery {
		c.recoverTopology()
	}

	c.recoverListeners(conn)

	c.notifyRecoveryListeners(conn)

	rabbitmq.Logger.Println("connection recovered")
}

func (c *connection) recoverConnection() (*amqp.Connection, *amqp.Error) {
	c.backoff.Reset()

	for {
		duration := c.backoff.NextBackOff()
		if duration == backoff.Stop {
			return nil, ErrRecoverBackOff
		}

		time.Sleep(duration)

		conn, err := rabbitmq.NewConnection(c.config, c.balancer)
		if err != nil {
			rabbitmq.Logger.Println("error on reconnect:", err)
			continue
		}

		return conn, nil
	}
}

func (c *connection) recoverChannels(conn *amqp.Connection) {
	c.channelsMx.RLock()

	for ch := range c.channels {
		if err := ch.automaticallyRecover(conn); err != nil {
			c.errorHandler.HandleChannelRecoveryError(ch, err)
		}
	}
	c.channelsMx.RUnlock()
}

func (c *connection) recoverTopology() {
	c.topologyMx.RLock()
	defer c.topologyMx.RUnlock()

	for id, exchange := range c.exchanges {
		rabbitmq.Logger.Println("exchange recovered:", id)

		if err := exchange.recover(); err != nil {
			c.errorHandler.HandleTopologyRecoveryError(c, err)
		}
	}

	for id, queue := range c.queues {
		rabbitmq.Logger.Println("queue recovered:", id)

		if err := queue.recover(); err != nil {
			c.errorHandler.HandleTopologyRecoveryError(c, err)
		}
	}

	for id, exchangeBinding := range c.exchangeBindings {
		rabbitmq.Logger.Println("exchange binding recovered:", id)

		if err := exchangeBinding.recover(); err != nil {
			c.errorHandler.HandleTopologyRecoveryError(c, err)
		}
	}

	for id, queueBinding := range c.queueBindings {
		rabbitmq.Logger.Println("queue binding recovered:", id)

		if err := queueBinding.recover(); err != nil {
			c.errorHandler.HandleTopologyRecoveryError(c, err)
		}
	}

	for tag, consumer := range c.consumers {
		rabbitmq.Logger.Println("consumer recovered:", tag)

		if err := consumer.recover(); err != nil {
			c.errorHandler.HandleTopologyRecoveryError(c, err)
		}
	}
}

func (c *connection) recoverListeners(conn *amqp.Connection) {
	c.notifyMx.RLock()
	for _, ch := range c.blocks {
		go blockingListener(conn, ch)
	}
	c.notifyMx.RUnlock()
}

func (c *connection) notifyRecoveryListeners(conn *amqp.Connection) {
	c.notifyMx.RLock()
	for _, ch := range c.recoveries {
		ch <- conn
	}
	c.notifyMx.RUnlock()
}

func (c *connection) recordQueueBinding(ch *channel, name, key, exchange string, args amqp.Table) {
	binding := newQueueBinding(ch, name, key, exchange, args)

	c.topologyMx.Lock()
	c.queueBindings[binding.string()] = binding
	c.topologyMx.Unlock()
}

func (c *connection) deleteRecordedQueueBinding(ch *channel, name, key, exchange string, args amqp.Table) {
	binding := newQueueBinding(ch, name, key, exchange, args)

	c.topologyMx.Lock()
	delete(c.queueBindings, binding.string())
	c.topologyMx.Unlock()
}

func (c *connection) recordExchangeBinding(ch *channel, destination, key, source string, args amqp.Table) {
	binding := newExchangeBinding(ch, destination, key, source, args)

	c.topologyMx.Lock()
	c.exchangeBindings[binding.String()] = binding
	c.topologyMx.Unlock()
}

func (c *connection) deleteRecordedExchangeBinding(ch *channel, destination, key, source string, args amqp.Table) {
	binding := newExchangeBinding(ch, destination, key, source, args)

	c.topologyMx.Lock()
	delete(c.exchangeBindings, binding.String())
	c.topologyMx.Unlock()
}

func (c *connection) recordQueue(ch *channel, name string, durable, autoDelete, exclusive bool, args amqp.Table) {
	c.topologyMx.Lock()
	c.queues[name] = newQueue(ch, name, durable, autoDelete, exclusive, args)
	c.topologyMx.Unlock()
}

func (c *connection) deleteRecordedQueue(name string) {
	c.topologyMx.Lock()
	delete(c.queues, name)
	c.topologyMx.Unlock()
}

func (c *connection) recordExchange(ch *channel, name, kind string, durable, autoDelete, internal bool, args amqp.Table) {
	c.topologyMx.Lock()
	c.exchanges[name] = newExchange(ch, name, kind, durable, autoDelete, internal, args)
	c.topologyMx.Unlock()
}

func (c *connection) deleteRecordedExchange(name string) {
	c.topologyMx.Lock()
	delete(c.exchanges, name)
	c.topologyMx.Unlock()
}

func (c *connection) recordConsumer(consumerTag string, consumer *consumer) {
	c.topologyMx.Lock()
	c.consumers[consumerTag] = consumer
	c.topologyMx.Unlock()
}

func (c *connection) retrieveRecordedConsumer(tag string) *consumer {
	c.topologyMx.Lock()
	defer c.topologyMx.Unlock()

	if cons, ok := c.consumers[tag]; ok {
		delete(c.consumers, tag)
		return cons
	}

	return nil
}

func (c *connection) IsClosed() bool {
	return c.delegate.load().IsClosed()
}

func (c *connection) Close() error {
	err := c.delegate.load().Close()

	c.shutdown(nil)

	return err
}

func (c *connection) shutdown(err *amqp.Error) {
	c.destructor.Do(func() {
		c.notifyMx.RLock()

		if err != nil {
			for _, c := range c.closes {
				c <- err
			}
		}

		for _, c := range c.closes {
			close(c)
		}

		for _, c := range c.blocks {
			close(c)
		}

		for _, c := range c.recoveries {
			close(c)
		}

		c.notifyMx.RUnlock()

		c.channelsMx.RLock()

		for ch := range c.channels {
			ch.shutdown(err)
		}

		c.channelsMx.RUnlock()

		rabbitmq.Logger.Println("connection closed")
	})
}
