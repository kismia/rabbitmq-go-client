package recovery

import amqp "github.com/rabbitmq/amqp091-go"

func blockingListener(conn *amqp.Connection, l chan amqp.Blocking) {
	for n := range conn.NotifyBlocked(make(chan amqp.Blocking)) {
		l <- n
	}
}

func flowListener(ch *amqp.Channel, l chan bool) {
	for n := range ch.NotifyFlow(make(chan bool)) {
		l <- n
	}
}

func returnListener(ch *amqp.Channel, l chan amqp.Return) {
	for n := range ch.NotifyReturn(make(chan amqp.Return)) {
		l <- n
	}
}

func cancelListener(ch *amqp.Channel, l chan string) {
	for n := range ch.NotifyCancel(make(chan string)) {
		l <- n
	}
}

func publishListener(ch *amqp.Channel, l chan amqp.Confirmation) {
	for n := range ch.NotifyPublish(make(chan amqp.Confirmation)) {
		l <- n
	}
}

func confirmListeners(ch *amqp.Channel, ack, nack chan uint64) {
	a, n := ch.NotifyConfirm(ack, nack)

	go func() {
		for n := range a {
			ack <- n
		}
	}()

	go func() {
		for n := range n {
			nack <- n
		}
	}()
}
