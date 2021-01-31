package amqp

import (
	"fmt"
	"github.com/astaxie/beego/logs"
	"github.com/streadway/amqp"
)

type Producer struct {
	amqpURI         string
	exchange        string
	exchangeType    string
	routingKey      string
	reliable        bool
	ExchangeDeclare func(channel *amqp.Channel, exchange, exchangeType string) error
}

func NewProducer(amqpURI string, exchange string, exchangeType string, routingKey string, reliable bool) *Producer {
	return &Producer{amqpURI: amqpURI, exchange: exchange, exchangeType: exchangeType, routingKey: routingKey, reliable: reliable}
}

func (p *Producer) Publish(contentType string, body []byte) error {
	conn, err := amqp.Dial(p.amqpURI)
	if err != nil {
		return fmt.Errorf("create connection err: %s", err)
	}

	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("create channel err: %s", err)
	}

	if p.ExchangeDeclare != nil {
		if err = p.ExchangeDeclare(channel, p.exchange, p.exchangeType); err != nil {
			return fmt.Errorf("exchange declare err : %s", err)
		}
	} else {
		if err := channel.ExchangeDeclare(
			p.exchange,
			p.exchangeType,
			true,
			false,
			false,
			false,
			nil); err != nil {
			return fmt.Errorf("exchange declare: %s", err)
		}
	}

	if p.reliable {
		if err := channel.Confirm(false); err != nil {
			return fmt.Errorf("channel could not be put into confirm mode: %s", err)
		}
		// 缓冲区为1
		confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer p.confirmOne(confirms)
	}

	if err = channel.Publish(
		p.exchange,
		p.routingKey,
		false, // 无法路由到合适队列 不返回消息
		false, // 队列无消费者 不返回消息
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     contentType,
			ContentEncoding: "",
			Body:            body,
			DeliveryMode:    amqp.Transient,
			Priority:        0,
		}); err != nil {
		return fmt.Errorf("exchange publish: %s", err)
	}
	return nil
}

func (p *Producer) confirmOne(confirms <-chan amqp.Confirmation) {
	if confirmed := <-confirms; !confirmed.Ack {
		logs.Error("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}
