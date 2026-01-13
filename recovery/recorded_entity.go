package recovery

import (
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

type exchange struct {
	channel                       *channel
	name, kind                    string
	durable, autoDelete, internal bool
	args                          amqp.Table
}

func newExchange(ch *channel, name, kind string, durable, autoDelete, internal bool, args amqp.Table) *exchange {
	return &exchange{
		channel:    ch,
		name:       name,
		kind:       kind,
		durable:    durable,
		autoDelete: autoDelete,
		internal:   internal,
		args:       args,
	}
}

func (e *exchange) recover() error {
	return e.channel.delegate.load().ExchangeDeclare(e.name, e.kind, e.durable, e.autoDelete, e.internal, false, e.args)
}

type exchangeBinding struct {
	channel                  *channel
	destination, key, source string
	args                     amqp.Table
}

func newExchangeBinding(ch *channel, destination, key, source string, args amqp.Table) *exchangeBinding {
	return &exchangeBinding{
		channel:     ch,
		destination: destination,
		key:         key,
		source:      source,
		args:        args,
	}
}

func (eb *exchangeBinding) recover() error {
	return eb.channel.delegate.load().ExchangeBind(eb.destination, eb.key, eb.source, false, eb.args)
}

func (eb *exchangeBinding) String() string {
	b := strings.Builder{}
	b.WriteString(eb.destination)
	b.WriteRune('-')
	b.WriteString(eb.key)
	b.WriteRune('-')
	b.WriteString(eb.source)

	return b.String()
}

type queue struct {
	channel                        *channel
	name                           string
	durable, autoDelete, exclusive bool
	args                           amqp.Table
}

func newQueue(ch *channel, name string, durable, autoDelete, exclusive bool, args amqp.Table) *queue {
	return &queue{
		channel:    ch,
		name:       name,
		durable:    durable,
		autoDelete: autoDelete,
		exclusive:  exclusive,
		args:       args,
	}
}

func (q *queue) recover() error {
	_, err := q.channel.delegate.load().QueueDeclare(q.name, q.durable, q.autoDelete, q.exclusive, false, q.args)
	return err
}

type queueBinding struct {
	channel             *channel
	name, key, exchange string
	args                amqp.Table
}

func newQueueBinding(ch *channel, name, key, exchange string, args amqp.Table) *queueBinding {
	return &queueBinding{
		channel:  ch,
		name:     name,
		key:      key,
		exchange: exchange,
		args:     args,
	}
}

func (qb *queueBinding) recover() error {
	return qb.channel.delegate.load().QueueBind(qb.name, qb.key, qb.exchange, false, qb.args)
}

func (qb *queueBinding) string() string {
	b := strings.Builder{}
	b.WriteString(qb.name)
	b.WriteRune('-')
	b.WriteString(qb.key)
	b.WriteRune('-')
	b.WriteString(qb.exchange)

	return b.String()
}

type consumer struct {
	channel                     *channel
	deliveries                  chan amqp.Delivery
	queue, tag                  string
	autoAck, exclusive, noLocal bool
	args                        amqp.Table
}

func newConsumer(ch *channel, ds <-chan amqp.Delivery, queue, tag string, autoAck, exclusive, noLocal bool, args amqp.Table) *consumer {
	c := &consumer{
		channel:    ch,
		deliveries: make(chan amqp.Delivery),
		queue:      queue,
		tag:        tag,
		autoAck:    autoAck,
		exclusive:  exclusive,
		noLocal:    noLocal,
		args:       args,
	}

	go c.forward(ds)

	return c
}

func (c *consumer) recover() error {
	ds, err := c.channel.delegate.load().Consume(c.queue, c.tag, c.autoAck, c.exclusive, c.noLocal, false, c.args)
	if err != nil {
		return err
	}

	go c.forward(ds)

	return nil
}

func (c *consumer) forward(ds <-chan amqp.Delivery) {
	for d := range ds {
		c.deliveries <- d
	}
}

func (c *consumer) cancel(consumer string, noWait bool) error {
	defer close(c.deliveries)

	return c.channel.delegate.load().Cancel(consumer, noWait)
}
