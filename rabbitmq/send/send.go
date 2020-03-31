package MqSend

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

func PushMsg(exchange,key,data string)  {
	conn,err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Println("创建连接失败",err)
		return
	}
	defer conn.Close()
	c,err := conn.Channel()
	if err != nil {
		log.Println("开启通道失败")
		return
	}
	log.Printf("开始推送:%v",time.Now())
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:time.Now(),
		ContentType:"application/json",
		Body:[]byte(data),}
	err = c.Publish(exchange, key, false, false, msg)
	if err != nil {
		log.Fatalf("basic.publish: %v", err)}
	log.Printf("推送成功:%s", time.Now())
}