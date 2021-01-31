package amqp

import (
	"github.com/astaxie/beego/logs"
	"github.com/streadway/amqp"
	"time"
)

type Consumer struct {
	amqpURI       string
	queue         string
	exchange      string
	exchangeType  string
	routingKey    string
	tag           string
	conn          *amqp.Connection
	channel       *amqp.Channel
	connNotify    chan *amqp.Error
	channelNotify chan *amqp.Error
	quit          chan bool

	ExchangeDeclare func(channel *amqp.Channel, exchange, exchangeType string) error
	QueueDeclare    func(channel *amqp.Channel, queue string) error
	QueueBind       func(channel *amqp.Channel, queue, routingKey, exchange string) error
	Consume         func(channel *amqp.Channel, queue, tag string) (<-chan amqp.Delivery, error)
	MessageHandler  func(Body []byte) error
}

func NewConsumer(amqpURI string, queue string, exchange string, exchangeType string, routingKey string, tag string) *Consumer {
	return &Consumer{amqpURI: amqpURI, queue: queue, exchange: exchange, exchangeType: exchangeType, routingKey: routingKey, tag: tag}
}

func (c *Consumer) Start() error {
	if err := c.Run(); err != nil {
		return err
	}
	go c.ReConnect()
	return nil
}

func (c *Consumer) Run() error {
	var err error
	// 建立连接
	if c.conn, err = amqp.Dial(c.amqpURI); err != nil {
		return err
	}

	// 创建channel
	if c.channel, err = c.conn.Channel(); err != nil {
		return err
	}

	// 交换机类型声明
	if c.ExchangeDeclare != nil {
		if err = c.ExchangeDeclare(c.channel, c.exchange, c.exchangeType); err != nil {
			c.Close()
			return err
		}
	} else {
		if err = c.channel.ExchangeDeclare(
			c.exchange,
			c.exchangeType,
			true,  // 持久化
			false, // 1. 曾经有交换机/队列连接过 2. 现在没有与之链接的交换机/队列 不自动删除
			false, // 可以直接发送消息到交换机
			false, // 不等待声明结果
			nil,
		); err != nil {
			c.Close()
			return err
		}
	}

	// 队列声明
	if c.QueueDeclare != nil {
		if err = c.QueueDeclare(c.channel, c.queue); err != nil {
			c.Close()
			return err
		}
	} else {
		if _, err = c.channel.QueueDeclare(
			c.queue,
			true,  // 持久化
			false, // 1. 曾经有消费者连接过 2. 现在没有消费者连接 不删除队列
			false, // 排他队列
			false,
			nil); err != nil {
			c.Close()
			return err
		}
	}

	if c.QueueBind != nil {
		if err = c.QueueBind(c.channel, c.queue, c.routingKey, c.exchange); err != nil {
			c.Close()
			return err
		}
	} else {
		if err = c.channel.QueueBind(c.queue, c.routingKey, c.exchange, false, nil); nil != err {
			c.Close()
			return err
		}
	}

	var delivery <-chan amqp.Delivery
	if c.Consume != nil {
		if delivery, err = c.Consume(c.channel, c.queue, c.tag); err != nil {
			c.Close()
			return err
		}
	} else {
		if delivery, err = c.channel.Consume(
			c.queue,
			c.tag,
			false,
			false,
			false,
			false,
			nil); err != nil {
			c.Close()
			return err
		}
	}

	go c.Handle(delivery)

	c.connNotify = c.conn.NotifyClose(make(chan *amqp.Error))
	c.channelNotify = c.channel.NotifyClose(make(chan *amqp.Error))
	return err
}

func (c *Consumer) Handle(delivery <-chan amqp.Delivery) {
	for d := range delivery {
		if c.MessageHandler == nil {
			d.Reject(true)
		} else {
			if err := c.MessageHandler(d.Body); err == nil {
				d.Ack(false)
			} else {
				d.Reject(true)
			}
		}
	}
}

func (c *Consumer) ReConnect() {
	for {
		select {
		case err := <-c.connNotify:
			if err != nil {
				logs.Error("rabbitmq consumer - connection NotifyClose:", err)
			}
		case err := <-c.channelNotify:
			if err != nil {
				logs.Error("rabbitmq consumer - connection NotifyClose:", err)
			}
		case <-c.quit:
			return
		}

		if !c.conn.IsClosed() {
			if err := c.channel.Cancel(c.tag, true); err != nil {
				logs.Error("rabbitmq consumer - channel cancel failed: ", err)
			}

			if err := c.conn.Close(); err != nil {
				logs.Error("rabbitmq consumer - channel cancel failed: ", err)
			}
		}

		for err := range c.channelNotify {
			logs.Error(err)
		}

		for err := range c.connNotify {
			logs.Error(err)
		}

	quit:
		for {
			select {
			case <-c.quit:
				return
			default:
				logs.Info("rabbitmq consumer - reconnect")
				if err := c.Run(); err != nil {
					logs.Error("rabbitmq consumer - failCheck: ", err)

					time.Sleep(time.Second * 5)
					continue
				}
				break quit
			}
		}
	}
}

func (c *Consumer) Wait() {
	<-c.quit
}

func (c *Consumer) Close() {
	_ = c.channel.Close()
	_ = c.conn.Close()
}
