package factory

import (
	"strconv"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Exchange struct {
	name      string
	queueName string
	ch        *amqp.Channel
	conn      *amqp.Connection
	keys      []string
	tag       string
}

func CreateExchange(exchangeName string, host string, port int, keys []string) (m.Middleware, error) {
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
		"direct",     // type
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
	for _, key := range keys {
		err = ch.QueueBind(q.Name, key, exchangeName, false, nil)
		if err != nil {
			return nil, m.ErrMessageMiddlewareMessage
		}
	}
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}
	return &Exchange{name: exchangeName, queueName: q.Name, ch: ch, conn: conn, keys: keys}, nil
}

// Comienza a escuchar a la cola/exchange e invoca a callbackFunc tras
// cada mensaje de datos o de control con el cuerpo del mensaje.
// callbackFunc tiene como parámetro:
// msg - El struct tal y como lo recibe el método Send.
// ack - Una función que hace ACK del mensaje recibido.
// nack - Una función que hace NACK del mensaje recibido.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (e *Exchange) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {

	// genero un tag unico para identificar al consumidor
	e.tag = strconv.FormatInt(time.Now().UnixNano(), 10)
	mesagges, err := e.ch.Consume(
		e.queueName,
		e.tag, // consumer
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
	if e.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

// Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
// no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
func (e *Exchange) StopConsuming() error {
	err := e.ch.Cancel(e.tag, false)
	if err != nil {
		if e.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
	}
	return nil
}

// Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (e *Exchange) Send(msg m.Message) (err error) {
	for _, key := range e.keys {
		err = e.ch.Publish(
			e.name, // exchange
			key,    // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})

		if err != nil {
			if e.conn.IsClosed() {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}
	return nil
}

// Se desconecta de la cola o exchange al que estaba conectado.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareClose.
func (e *Exchange) Close() error {
	errChan := e.ch.Close()
	errConn := e.conn.Close()

	if errChan != nil || errConn != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
