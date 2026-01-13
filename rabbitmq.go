package rabbitmq

import (
	"io"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

var Logger StdLogger = log.New(io.Discard, "[rabbitmq] ", log.LstdFlags)

type Connection interface {
	Channel() (Channel, error)
	NotifyBlocked(receiver chan amqp.Blocking) chan amqp.Blocking
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	IsClosed() bool
	Close() error
}

type Channel interface {
	Qos(prefetchCount, prefetchSize int, global bool) error
	Cancel(consumer string, noWait bool) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	QueueUnbind(name, key, exchange string, args amqp.Table) error
	QueuePurge(name string, noWait bool) (int, error)
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDelete(name string, ifUnused, noWait bool) error
	ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error
	ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Get(queue string, autoAck bool) (msg amqp.Delivery, ok bool, err error)
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Tx() error
	TxCommit() error
	TxRollback() error
	Flow(active bool) error
	Confirm(noWait bool) error
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple, requeue bool) error
	Reject(tag uint64, requeue bool) error
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	NotifyFlow(c chan bool) chan bool
	NotifyReturn(c chan amqp.Return) chan amqp.Return
	NotifyCancel(c chan string) chan string
	NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64)
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	Close() error
}

type RecoverableConnection interface {
	Connection
	NotifyRecovery(c chan *amqp.Connection) chan *amqp.Connection
}

type RecoverableChannel interface {
	Channel
	NotifyRecovery(c chan *amqp.Channel) chan *amqp.Channel
}

type Balancer interface {
	Balance() string
}

type ErrorHandler interface {
	HandleConnectionRecoveryError(c Connection, err error)
	HandleChannelRecoveryError(ch Channel, err error)
	HandleTopologyRecoveryError(c Connection, err error)
}

type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}
