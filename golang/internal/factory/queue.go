package factory

import (
	"strconv"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue struct {
	name  string
	queue amqp.Queue
	ch    *amqp.Channel
	conn  *amqp.Connection
}

func CreateQueue(queueName string, host string, port int) (m.Middleware, error) {

	portToConnect := strconv.Itoa(port)
	conn, err := amqp.Dial("amqp://guest:guest@" + host + ":" + portToConnect + "/")
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, m.ErrMessageMiddlewareMessage
	}

	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,
	)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, m.ErrMessageMiddlewareMessage
	}
	return Queue{queueName, q, ch, conn}, nil
}

// Comienza a escuchar a la cola/exchange e invoca a callbackFunc tras
// cada mensaje de datos o de control con el cuerpo del mensaje.
// callbackFunc tiene como parámetro:
// msg - El struct tal y como lo recibe el método Send.
// ack - Una función que hace ACK del mensaje recibido.
// nack - Una función que hace NACK del mensaje recibido.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (q Queue) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {

	mesagges, err := q.ch.Consume(
		q.name,
		q.name, // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		if q.conn != nil {
			q.conn.Close()
		}
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
			if q.conn != nil {
				q.conn.Close()
			}
			return m.ErrMessageMiddlewareMessage
		}
	}
	if q.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

// Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
// no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
func (q Queue) StopConsuming() error {
	err := q.ch.Cancel(q.name, false)
	if err != nil {
		if q.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
	}
	return nil
}

// Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (q Queue) Send(msg m.Message) (err error) {
	err = q.ch.Publish(
		"",     // exchange
		q.name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		if q.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

// Se desconecta de la cola o exchange al que estaba conectado.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareClose.
func (q Queue) Close() error {
	errChan := q.ch.Close()
	errConn := q.conn.Close()

	if errChan != nil || errConn != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
