package net

import (
	"amqp_tools/amqp"
	"amqp_tools/config"
	"fmt"
	"github.com/astaxie/beego/logs"

	"time"
)

var (
	mqSendingQueue = make(chan []byte, 10)
)

func MQInit() {
	RabbitmqProducer()
	go func() {
		RabbitmqConsumer()
	}()
}

func RabbitmqConsumer() {
	amqpUrl := fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		config.GlobalConfig.Username,
		config.GlobalConfig.Password,
		config.GlobalConfig.Host,
		config.GlobalConfig.Port,
		config.GlobalConfig.Vhost)
	consumer := amqp.NewConsumer(amqpUrl,
		config.GlobalConfig.Queue,
		config.GlobalConfig.Exchange,
		config.GlobalConfig.ExchangeType,
		config.GlobalConfig.RoutingKey,
		config.GlobalConfig.Tag)
	process := new(MsgProcess)
	consumer.MessageHandler = process.doProcess

reconnect:
	if err := consumer.Start(); err != nil {
		logs.Error(err)
		time.Sleep(time.Second * 15)
		goto reconnect
	}
	consumer.Wait()
}

func RabbitmqProducer() {
	amqpUrl := fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		config.GlobalConfig.Username,
		config.GlobalConfig.Password,
		config.GlobalConfig.Host,
		config.GlobalConfig.Port,
		config.GlobalConfig.Vhost)
	producer := amqp.NewProducer(amqpUrl,
		config.GlobalConfig.Exchange,
		config.GlobalConfig.ExchangeType,
		config.GlobalConfig.RoutingKey,
		config.GlobalConfig.Reliable)
	go func() {
		for {
			msg := <-mqSendingQueue
			producer.Publish("text/plain", msg)
		}
	}()
}

func SendMessage(msg []byte) {
	logs.Info(fmt.Sprintf("send msg %s", string(msg)))
	mqSendingQueue <- msg
}
