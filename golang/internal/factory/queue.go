package factory

import (
	"log"
	"strconv"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue struct {
	name  string
	queue amqp.Queue
	ch    *amqp.Channel
}

func CreateQueue(queueName string, host string, port int) (m.Middleware, error) {

	portToConnect := strconv.Itoa(port)
	conn, err := amqp.Dial("amqp://guest:guest@" + host + ":" + portToConnect + "/")
	failOnError(err, "Failed to connect to RabbitMQ")
	// defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	// defer ch.Close()

	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	failOnError(err, "Failed to declare a queue")
	return Queue{queueName, q, ch}, nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func (q Queue) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	return
}

// Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
// no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
func (q Queue) StopConsuming() {

}

// Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (q Queue) Send(msg m.Message) (err error) {
	return
}

// Se desconecta de la cola o exchange al que estaba conectado.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareClose.
func (q Queue) Close() error {
	err := q.ch.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
