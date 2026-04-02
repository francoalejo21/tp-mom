package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return CreateQueue(queueName, connectionSettings.Hostname, connectionSettings.Port)
}

func CreateExchangeMiddleware(exchangeName string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return CreateExchange(exchangeName, connectionSettings.Hostname, connectionSettings.Port, keys)
}
