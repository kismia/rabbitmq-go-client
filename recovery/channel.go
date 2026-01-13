package recovery

import (
	"sync"

	"github.com/kismia/rabbitmq-go-client"
	amqp "github.com/rabbitmq/amqp091-go"
)

type channel struct {
	connection          *connection
	delegate            *atomicChannel
	prefetchCount       int
	prefetchCountGlobal int
	confirm             bool
	transaction         bool
	consumerTagsMx      sync.RWMutex
	consumerTags        []string
	notifyMx            sync.RWMutex
	recoveries          []chan *amqp.Channel
	flows               []chan bool
	returns             []chan amqp.Return
	cancels             []chan string
	confirms            []chan amqp.Confirmation
	acks, nacks         []chan uint64
	closes              []chan *amqp.Error
	destructor          sync.Once
}

func newChannel(conn *connection, delegate *amqp.Channel) *channel {
	return &channel{
		connection: conn,
		delegate:   newAtomicChannel(delegate),
	}
}

func (ch *channel) Qos(prefetchCount, prefetchSize int, global bool) error {
	if global {
		ch.prefetchCountGlobal = prefetchCount
	} else {
		ch.prefetchCount = prefetchCount
	}

	return ch.delegate.load().Qos(prefetchCount, prefetchSize, global)
}

func (ch *channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	queue, err := ch.delegate.load().QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		return queue, err
	}

	ch.connection.recordQueue(ch, name, durable, autoDelete, exclusive, args)

	return queue, nil
}

func (ch *channel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return ch.delegate.load().QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}

func (ch *channel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	err := ch.delegate.load().QueueBind(name, key, exchange, noWait, args)
	if err != nil {
		return err
	}

	ch.connection.recordQueueBinding(ch, name, key, exchange, args)

	return nil
}

func (ch *channel) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	ch.connection.deleteRecordedQueueBinding(ch, name, key, exchange, args)

	return ch.delegate.load().QueueUnbind(name, key, exchange, args)
}

func (ch *channel) QueuePurge(name string, noWait bool) (int, error) {
	return ch.delegate.load().QueuePurge(name, noWait)
}

func (ch *channel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	ch.connection.deleteRecordedQueue(name)

	return ch.delegate.load().QueueDelete(name, ifUnused, ifEmpty, false)
}

func (ch *channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	ds, err := ch.delegate.load().Consume(queue, consumer, autoAck, exclusive, noWait, noWait, args)
	if err != nil {
		return nil, err
	}

	if !ch.connection.topologyRecovery {
		return ds, nil
	}

	ch.consumerTagsMx.Lock()
	ch.consumerTags = append(ch.consumerTags, consumer)
	ch.consumerTagsMx.Unlock()

	rc := newConsumer(ch, ds, queue, consumer, autoAck, exclusive, noLocal, args)
	ch.connection.recordConsumer(consumer, rc)

	return rc.deliveries, nil
}

func (ch *channel) Cancel(consumer string, noWait bool) error {
	if rc := ch.connection.retrieveRecordedConsumer(consumer); rc != nil {
		return rc.cancel(consumer, noWait)
	}

	return ch.delegate.load().Cancel(consumer, noWait)
}

func (ch *channel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	err := ch.delegate.load().ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
	if err != nil {
		return err
	}

	ch.connection.recordExchange(ch, name, kind, durable, autoDelete, internal, args)

	return nil
}

func (ch *channel) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return ch.delegate.load().ExchangeDeclarePassive(name, kind, durable, autoDelete, internal, noWait, args)
}

func (ch *channel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	ch.connection.deleteRecordedExchange(name)

	return ch.delegate.load().ExchangeDelete(name, ifUnused, noWait)
}

func (ch *channel) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	err := ch.delegate.load().ExchangeBind(destination, key, source, noWait, args)
	if err != nil {
		return err
	}

	ch.connection.recordExchangeBinding(ch, destination, key, source, args)

	return nil
}

func (ch *channel) ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error {
	ch.connection.deleteRecordedExchangeBinding(ch, destination, key, source, args)

	return ch.delegate.load().ExchangeUnbind(destination, key, source, noWait, args)
}

func (ch *channel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return ch.delegate.load().Publish(exchange, key, mandatory, immediate, msg)
}

func (ch *channel) Get(queue string, autoAck bool) (msg amqp.Delivery, ok bool, err error) {
	return ch.delegate.load().Get(queue, autoAck)
}

func (ch *channel) Tx() error {
	ch.transaction = true
	return ch.delegate.load().Tx()
}

func (ch *channel) TxCommit() error {
	return ch.delegate.load().TxCommit()
}

func (ch *channel) TxRollback() error {
	return ch.delegate.load().TxRollback()
}

func (ch *channel) Flow(active bool) error {
	return ch.delegate.load().Flow(active)
}

func (ch *channel) Confirm(noWait bool) error {
	ch.confirm = true
	return ch.delegate.load().Confirm(noWait)
}

func (ch *channel) Ack(tag uint64, multiple bool) error {
	return ch.delegate.load().Ack(tag, multiple)
}

func (ch *channel) Nack(tag uint64, multiple, requeue bool) error {
	return ch.delegate.load().Nack(tag, multiple, requeue)
}

func (ch *channel) Reject(tag uint64, requeue bool) error {
	return ch.delegate.load().Reject(tag, requeue)
}

func (ch *channel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	ch.notifyMx.Lock()
	ch.closes = append(ch.closes, c)
	ch.notifyMx.Unlock()

	return c
}

func (ch *channel) NotifyFlow(c chan bool) chan bool {
	ch.notifyMx.Lock()
	ch.flows = append(ch.flows, c)
	ch.notifyMx.Unlock()

	go flowListener(ch.delegate.load(), c)

	return c
}

func (ch *channel) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	ch.notifyMx.Lock()
	ch.returns = append(ch.returns, c)
	ch.notifyMx.Unlock()

	go returnListener(ch.delegate.load(), c)

	return c
}

func (ch *channel) NotifyCancel(c chan string) chan string {
	ch.notifyMx.Lock()
	ch.cancels = append(ch.cancels, c)
	ch.notifyMx.Unlock()

	go cancelListener(ch.delegate.load(), c)

	return c
}

func (ch *channel) NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64) {
	ch.notifyMx.Lock()
	ch.acks = append(ch.acks, ack)
	ch.nacks = append(ch.nacks, nack)
	ch.notifyMx.Unlock()

	confirmListeners(ch.delegate.load(), ack, nack)

	return ack, nack
}

func (ch *channel) NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation {
	ch.notifyMx.Lock()
	ch.confirms = append(ch.confirms, c)
	ch.notifyMx.Unlock()

	go publishListener(ch.delegate.load(), c)

	return c
}

func (ch *channel) NotifyRecovery(c chan *amqp.Channel) chan *amqp.Channel {
	ch.notifyMx.Lock()
	ch.recoveries = append(ch.recoveries, c)
	ch.notifyMx.Unlock()

	return c
}

func (ch *channel) Close() error {
	ch.connection.unregisterChannel(ch)

	err := ch.delegate.load().Close()

	ch.shutdown(nil)

	return err
}

func (ch *channel) shutdown(err *amqp.Error) {
	ch.destructor.Do(func() {
		ch.notifyMx.RLock()
		if err != nil {
			for _, c := range ch.closes {
				c <- err
			}
		}
		ch.notifyMx.RUnlock()

		ch.consumerTagsMx.RLock()
		for _, tag := range ch.consumerTags {
			if cons := ch.connection.retrieveRecordedConsumer(tag); cons != nil {
				close(cons.deliveries)
			}
		}
		ch.consumerTagsMx.RUnlock()

		ch.notifyMx.RLock()

		for _, c := range ch.closes {
			close(c)
		}

		for _, c := range ch.recoveries {
			close(c)
		}

		for _, c := range ch.flows {
			close(c)
		}

		for _, c := range ch.returns {
			close(c)
		}

		for _, c := range ch.cancels {
			close(c)
		}

		for _, c := range ch.confirms {
			close(c)
		}

		for i, ack := range ch.acks {
			close(ack)
			close(ch.nacks[i])
		}

		ch.notifyMx.RUnlock()

		rabbitmq.Logger.Println("channel closed")
	})
}

func (ch *channel) automaticallyRecover(delegateConn *amqp.Connection) error {
	newChannel, err := delegateConn.Channel()
	if err != nil {
		return err
	}

	defunctChannel := ch.delegate.load()

	ch.delegate.store(newChannel)

	defunctChannel.Close()

	ch.recoverListeners(newChannel)

	if err = ch.recoverState(newChannel); err != nil {
		return err
	}

	ch.notifyRecoveryListeners(newChannel)

	return nil
}

func (ch *channel) recoverState(amqpCh *amqp.Channel) error {
	if ch.prefetchCountGlobal > 0 {
		if err := amqpCh.Qos(ch.prefetchCountGlobal, 0, true); err != nil {
			return err
		}
	}

	if ch.prefetchCount > 0 {
		if err := amqpCh.Qos(ch.prefetchCount, 0, false); err != nil {
			return err
		}
	}

	if ch.confirm {
		if err := amqpCh.Confirm(false); err != nil {
			return err
		}
	}

	if ch.transaction {
		if err := amqpCh.Tx(); err != nil {
			return err
		}
	}

	return nil
}

func (ch *channel) recoverListeners(amqpCh *amqp.Channel) {
	ch.notifyMx.RLock()

	for _, c := range ch.flows {
		go flowListener(amqpCh, c)
	}

	for _, c := range ch.returns {
		go returnListener(amqpCh, c)
	}

	for _, c := range ch.cancels {
		go cancelListener(amqpCh, c)
	}

	for _, c := range ch.confirms {
		go publishListener(amqpCh, c)
	}

	for i, ack := range ch.acks {
		confirmListeners(amqpCh, ack, ch.nacks[i])
	}

	ch.notifyMx.RUnlock()
}

func (ch *channel) notifyRecoveryListeners(amqpCh *amqp.Channel) {
	ch.notifyMx.RLock()
	for _, r := range ch.recoveries {
		r <- amqpCh
	}
	ch.notifyMx.RUnlock()
}
