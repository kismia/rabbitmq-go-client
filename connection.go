package rabbitmq

import (
	"net/url"

	amqp "github.com/rabbitmq/amqp091-go"
)

const amqpScheme = "amqp"

func NewConnection(config amqp.Config, balancer Balancer) (*amqp.Connection, error) {
	u := url.URL{
		Scheme: amqpScheme,
		Host:   balancer.Balance(),
	}

	s := u.String()

	Logger.Println("open connection to", s)

	return amqp.DialConfig(s, config)
}

type ConnectionAdapter struct {
	*amqp.Connection
}

func (c *ConnectionAdapter) Channel() (Channel, error) {
	return c.Connection.Channel()
}
