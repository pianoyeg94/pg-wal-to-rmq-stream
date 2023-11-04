package connectors

import "go.uber.org/zap"

type Connector interface {
	Start(logger *zap.Logger) error
	StartStreaming(processedMsgIdsCh <-chan string) (<-chan *Msg, error)
	StopStreaming() error
	Stop() error
}

func NewMsg(
	id string,
	exchange string,
	exchangeKind string,
	routingKey string,
	headers map[string]interface{},
	contentType string,
	contentEncoding string,
	deliveryMode string,
	body []byte,
) *Msg {
	return &Msg{
		Id:              id,
		Exchange:        exchange,
		ExchangeKind:    exchangeKind,
		RoutingKey:      routingKey,
		Headers:         headers,
		ContentType:     contentType,
		ContentEncoding: contentEncoding,
		DeliveryMode:    deliveryMode,
		Body:            body,
	}
}

type Msg struct {
	Id              string
	Exchange        string
	ExchangeKind    string
	RoutingKey      string
	Headers         map[string]interface{}
	ContentType     string
	ContentEncoding string
	DeliveryMode    string
	Body            []byte
}

type Retry struct {
	MsgId string
	Delay int64
}
