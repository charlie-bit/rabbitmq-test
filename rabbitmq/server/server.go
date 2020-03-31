package main

import (
	"flag"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"log"
	"rabbitmq/config"
	"rabbitmq/model"
	"rabbitmq/utils"
	"runtime"
	"sync"
	"time"
)

func main()  {
	configFileName := flag.String("c", "config/queues.example.yml", "config file path")
	// read yaml config
	var allQueues []*config.QueueConfig
	allQueues = config.LoadQueuesConfig(*configFileName, allQueues)
	//声明队列
	_, channel,err := utils.SetUpChannel()
	if err != nil {
		return
	}
	for _, queue := range allQueues {
		queue.DeclareExchange(channel)
		queue.DeclareQueue(channel)
	}
	done := make(chan struct{},2)
	utils.HandleSignal(done)
	//设置当前可以使用的cpu核
	log.Printf("当前可以使用的cpu核是：%d",runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())

	//首先接受信息
	<-resendMessage(ackMessage(workMessage(receiveMessage(*channel,allQueues,done))))
	//<-workMessage(receiveMessage(*channel,allQueues,done))
	log.Println("exiting program")
}
//接受信息
func receiveMessage(channel amqp.Channel,queues []*config.QueueConfig,done <-chan struct{}) <-chan model.Message {
	out := make(chan model.Message,model.ChannelBufferLength)
	var wg sync.WaitGroup
	receiver := func(qc config.QueueConfig) {
		defer wg.Done()
	RECONNECT:
		for{
			msgs,err := channel.Consume(qc.WorkerQueueName(),"",false,false,false,false,nil)
			if err != nil {
				log.Println("消费队列声明失败",err)
				return
			}
			for {
				select {
				case msg:=<-msgs:
					if m := string(msg.Body);m == "" {
						log.Println("消费端已无法消耗任何信息，连接可能断开")
						time.Sleep(time.Second * 5)
						continue RECONNECT
					}
					msg.MessageId = fmt.Sprintf("%s",uuid.NewV1())
					message := model.Message{QueueConfig:qc,AmqpDelivery:&msg}
					out <- message
					log.Printf("receive message:%s",string(msg.Body))
				case <-done:
					log.Println("receive a signal")
					return
				}
			}
		}
	}
	for _, queue := range queues {
		wg.Add(1)
		go receiver(*queue)
	}
	go func() {
		wg.Wait()
		log.Printf("消息处理完成，正在关闭管道...")
		close(out)
	}()
	return out
}

func workMessage(in <-chan model.Message) <-chan model.Message {
	var wg sync.WaitGroup
	out := make(chan model.Message, model.ChannelBufferLength)
	worker := func(m model.Message, o chan<- model.Message) {
		log.Printf("worker: received a msg, body: %s", string(m.AmqpDelivery.Body))
		defer wg.Done()
		m.Notify()
		o <- m
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for message := range in {
			wg.Add(1)
			go worker(message, out)
		}
	}()
	go func() {
		wg.Wait()
		log.Printf("all worker is done, closing channel")
		close(out)
	}()
	return out
}


func ackMessage(in <-chan model.Message) <-chan model.Message {
	out := make(chan model.Message)
	var wg sync.WaitGroup
	acker := func() {
		defer wg.Done()
		for m := range in {
			log.Printf("acker receive message:%s",string(m.AmqpDelivery.Body))
			if m.IsNotifySuccess() {
				_ = m.AmqpDelivery.Ack(false)
			}else if m.IsMaxRetry() {
				m.Republish(out)
			}else {
				m.Reject()
			}
		}
	}

	for i := 0; i<model.AckerNum ;i++  {
		wg.Add(1)
		go acker()
	}
	go func() {
		wg.Wait()
		log.Printf("消息处理完成，正在关闭acker...")
		close(out)
	}()
	return out
}

func resendMessage(in <-chan model.Message) <-chan model.Message {
	out := make(chan model.Message)

	var wg sync.WaitGroup

	resender := func() {
		defer wg.Done()

	RECONNECT:
		for {
			conn, channel, err := utils.SetUpChannel()
			if err != nil {
				log.Println(err)
				return
			}

			for m := range in {
				err := m.CloneAndPublish(channel)
				if err == amqp.ErrClosed {
					time.Sleep(5 * time.Second)
					continue RECONNECT
				}
			}

			// normally quit , we quit too
			conn.Close()
			break
		}
	}

	for i := 0; i < model.ResenderNum; i++ {
		wg.Add(1)
		go resender()
	}
	go func() {
		wg.Wait()
		log.Printf("all resender is done, close out")
		close(out)
	}()
	return out
}
