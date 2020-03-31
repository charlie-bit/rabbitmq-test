package model

import (
	"github.com/streadway/amqp"
	"log"
	MqBack "rabbitmq/Back"
	"rabbitmq/config"
	MqGet "rabbitmq/get"
	"rabbitmq/utils"
	"time"
)

type Message struct {
	QueueConfig config.QueueConfig
	AmqpDelivery *amqp.Delivery
	NotifyResponse interface{}
}
const (
	NotifySuccess = 1 //通知成功
	NotifyFailure = 0 //通知失败

	//worker number
	ReceiverNum = 1   //接受者数量
	AckerNum    = 1  //手动提交数量
	ResenderNum = 1   //重新发送数量
	// chan
	ChannelBufferLength = 1000 //通道长度
	WaitBack = 1 //未收到回馈等待时间 秒
)

//判断是否消息是否有成功响应
func (m Message) IsNotifySuccess() bool {
	if m.NotifyResponse == nil {
		for i := 0 ; i < WaitBack ; i ++  {
			time.Sleep(time.Second * WaitBack)
			m.NotifyResponse = MqBack.GetBack()
		}
	}
	if m.NotifyResponse == nil {
		m.NotifyResponse = NotifyFailure
	}
	return m.NotifyResponse.(int) == NotifySuccess
}
//判断是否达到了最大重试次数
func (m Message) IsMaxRetry() bool {
	retries := m.CurrentMessageRetries()
	maxRetries := m.QueueConfig.RetryTimesWithDefault()
	log.Println("尝试次数",retries)
	return retries >= maxRetries
}
//当前信息的重试次数
func (m Message) CurrentMessageRetries() int {
	msg := m.AmqpDelivery
	xDeathArray, ok := msg.Headers["x-death"].([]interface{})
	if !ok {
		log.Println("x-death array case fail")
		return 0
	}

	if len(xDeathArray) <= 0 {
		return 0
	}

	for _, h := range xDeathArray {
		xDeathItem := h.(amqp.Table)
		if xDeathItem["reason"] == "rejected" {
			return int(xDeathItem["count"].(int64))
		}
	}
	return 0
}
//再次发布
func (m Message) Republish(out chan<- Message) {
	log.Println("acker: ERROR republish message")
	out <- m
	m.AmqpDelivery.Ack(false)
}

//拒绝消息
func (m Message) Reject() {
	log.Println("acker: reject message")
	m.AmqpDelivery.Reject(false)
}

func (m Message) CloneAndPublish(channel *amqp.Channel) error {
	msg := m.AmqpDelivery
	qc := m.QueueConfig

	errMsg := utils.CloneToPublishMsg(msg)
	err := channel.Publish(qc.ErrorExchangeName(), msg.RoutingKey, false, false, *errMsg)
	return err
}
var param = make(map[string]string)
func (m *Message) Notify() *Message {
	qc := m.QueueConfig
	msg := m.AmqpDelivery
	//回复给消息队列的
	param["queue"] = qc.QueueName
	param["msg"] = string(msg.Body)
	MqGet.SendMsg(param)
	statusCode := MqBack.GetBack()
	if statusCode == nil {
		log.Println("等待消息回馈...")
		return m
	}
	if statusCode.(bool) {
		m.NotifyResponse = NotifySuccess
	}else {
		m.NotifyResponse = NotifyFailure
	}
	return m
}