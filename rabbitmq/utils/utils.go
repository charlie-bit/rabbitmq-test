package utils

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"syscall"
)
//拿到链接通道
func SetUpChannel() (*amqp.Connection, *amqp.Channel, error) {
	conn,err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Println("连接服务失败",err)
		return nil,nil,err
	}
	channel,err := conn.Channel()
	if err != nil {
		log.Println("开启通道",err)
		return nil,nil,err
	}
	err = channel.Qos(1, 0, false)
	if err != nil {
		log.Println("开启单个通道",err)
		return nil, nil, err
	}
	log.Println("setup channel success!")
	return conn, channel, nil
}

//捕获信号键入值
func HandleSignal(done chan<-struct{})  {
	sign := make(chan os.Signal,2)
	signal.Notify(sign,syscall.SIGINT)
	go func() {
		if s := <- sign; s!=nil {
			log.Printf("received a signal %v, close done channel", s)
			close(done)
		}
	}()
}

func CloneToPublishMsg(msg *amqp.Delivery) *amqp.Publishing {
	newMsg := amqp.Publishing{
		Headers: msg.Headers,

		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		MessageId:       msg.MessageId,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserId:          msg.UserId,
		AppId:           msg.AppId,

		Body: msg.Body,
	}

	return &newMsg
}