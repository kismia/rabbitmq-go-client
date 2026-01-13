package batch

import (
	"context"
	"errors"
	"time"

	"github.com/kismia/rabbitmq-go-client"
	"github.com/kismia/rabbitmq-go-client/util/identity"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DefaultDuration = time.Minute
	DefaultSize     = 1000
	DefaultTag      = "batch-consumer"
)

var ErrCanceled = errors.New("consumer canceled")

type Handler interface {
	HandleNext(amqp.Delivery)
	HandleCompleted() error
	HandleCrash()
}

type QueueBinding struct {
	Exchange string
	Key      string
}

type ConsumerOptions struct {
	Channel       rabbitmq.Channel
	Handler       Handler
	Exchange      string
	Queue         string
	Tag           string
	BatchSize     int
	BatchDuration time.Duration
	QueueBindings []QueueBinding
}

func (o ConsumerOptions) NewConsumer() *Consumer {
	if o.BatchSize == 0 {
		o.BatchSize = DefaultSize
	}

	if o.BatchDuration == 0 {
		o.BatchDuration = DefaultDuration
	}

	if len(o.Tag) == 0 {
		o.Tag = DefaultTag
	}

	o.Tag += "-" + identity.Generate()

	return &Consumer{
		opts: o,
		done: make(chan struct{}, 1),
	}
}

type Consumer struct {
	opts        ConsumerOptions
	deliveryTag uint64
	done        chan struct{}
}

func (c *Consumer) DeclareAndConsume() error {
	if err := c.Declare(); err != nil {
		return err
	}

	return c.Consume()
}

func (c *Consumer) Declare() error {
	queue, err := c.opts.Channel.QueueDeclare(c.opts.Queue, true, false, false, false, nil)
	if err != nil {
		return err
	}

	for _, bind := range c.opts.QueueBindings {
		if err := c.opts.Channel.QueueBind(queue.Name, bind.Key, bind.Exchange, false, nil); err != nil {
			return err
		}
	}

	return nil
}

func (c *Consumer) Consume() error {
	defer close(c.done)

	if err := c.opts.Channel.Qos(c.opts.BatchSize, 0, false); err != nil {
		return err
	}

	ds, err := c.opts.Channel.Consume(c.opts.Queue, c.opts.Tag, false, false, false, false, nil)
	if err != nil {
		return err
	}

	closeCh := c.opts.Channel.NotifyClose(make(chan *amqp.Error))

	for {
		err := c.nextBatch(ds, closeCh)
		if err == nil {
			continue
		}

		if err == ErrCanceled {
			return nil
		}

		return err
	}
}

func (c *Consumer) nextBatch(ds <-chan amqp.Delivery, closeCh <-chan *amqp.Error) error {
	ticker := time.NewTicker(c.opts.BatchDuration)
	defer ticker.Stop()

	var retrieved int

	for {
		select {
		case err := <-closeCh:
			c.opts.Handler.HandleCrash()

			return err
		case delivery, ok := <-ds:
			if !ok {
				if retrieved > 0 {
					if err := c.batchCompleted(); err != nil {
						return err
					}
				}

				return ErrCanceled
			}

			c.opts.Handler.HandleNext(delivery)
			c.deliveryTag = delivery.DeliveryTag

			retrieved++

			if retrieved >= c.opts.BatchSize {
				return c.batchCompleted()
			}
		case <-ticker.C:
			if retrieved > 0 {
				return c.batchCompleted()
			}

			return nil
		}
	}
}

func (c *Consumer) batchCompleted() error {
	if err := c.opts.Handler.HandleCompleted(); err != nil {
		c.opts.Channel.Nack(c.deliveryTag, true, true)

		return err
	}

	return c.opts.Channel.Ack(c.deliveryTag, true)
}

func (c *Consumer) Shutdown(ctx context.Context) error {
	if err := c.opts.Channel.Cancel(c.opts.Tag, false); err != nil {
		return err
	}

	for {
		select {
		case <-c.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
