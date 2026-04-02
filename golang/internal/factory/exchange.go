package factory

import (
	"strconv"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Exchange struct {
	name      string
	queueName string
	ch        *amqp.Channel
}

func CreateExchange(exchangeName string, host string, port int) (m.Middleware, error) {
	portToConnect := strconv.Itoa(port)
	conn, err := amqp.Dial("amqp://guest:guest@" + host + ":" + portToConnect + "/")
	if err != nil {
		return nil, m.ErrMessageMiddlewareClose
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		false,        // durability
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durability
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	err = ch.QueueBind(
		q.Name,       // queue name
		"",           // routing key
		exchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}
	return Exchange{exchangeName, q.Name, ch}, nil
}

// Comienza a escuchar a la cola/exchange e invoca a callbackFunc tras
// cada mensaje de datos o de control con el cuerpo del mensaje.
// callbackFunc tiene como parámetro:
// msg - El struct tal y como lo recibe el método Send.
// ack - Una función que hace ACK del mensaje recibido.
// nack - Una función que hace NACK del mensaje recibido.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (e Exchange) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {

	mesagges, err := e.ch.Consume(
		e.queueName,
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	for message := range mesagges {
		errComunication := false
		callbackFunc(
			m.Message{Body: string(message.Body)},
			func() {
				err := message.Ack(false)
				if err != nil {
					errComunication = true
				}
			},
			func() {
				err := message.Nack(false, false)
				if err != nil {
					errComunication = true
				}
			},
		)
		if errComunication {
			return m.ErrMessageMiddlewareMessage
		}
	}
	return nil
}

// Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
// no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
func (e Exchange) StopConsuming() error {
	err := e.ch.Cancel(e.name, true)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

// Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (e Exchange) Send(msg m.Message) (err error) {
	err = e.ch.Publish(
		e.name, // exchange
		"",     // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

// Se desconecta de la cola o exchange al que estaba conectado.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareClose.
func (e Exchange) Close() error {
	err := e.ch.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
