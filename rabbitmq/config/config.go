package config

import (
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type ProjectConfig struct {
	Name string 						`yaml:"name"`
	QueuesDefaultConfig QueuesDefaultConfig `yaml:"queues_default"`
	Queues              []QueueConfig       `yaml:"queues"`
}

type QueuesDefaultConfig struct {
	NotifyBase      string `yaml:"notify_base"`
	NotifyTimeout   int    `yaml:"notify_timeout"`
	RetryTimes      int    `yaml:"retry_times"`
	RetryDuration   int    `yaml:"retry_duration"`
	BindingExchange string `yaml:"binding_exchange"`
}
type QueueConfig struct {
	QueueName       string   `yaml:"queue_name"`
	RoutingKey      []string `yaml:"routing_key"`
	NotifyPath      string   `yaml:"notify_path"`
	NotifyTimeout   int      `yaml:"notify_timeout"`
	RetryTimes      int      `yaml:"retry_times"`
	RetryDuration   int      `yaml:"retry_duration"`
	BindingExchange string   `yaml:"binding_exchange"`
	project         *ProjectConfig
}

type ProjectsConfig struct {
	Projects []ProjectConfig `yaml:"projects"`
}

//读取配置文件信息
func LoadQueuesConfig(configFileName string, allQueues []*QueueConfig) []*QueueConfig {
	configFile, err := ioutil.ReadFile(configFileName)
	if err != nil {
		log.Println("无法读取配置文件",err)
		return nil
	}
	projectsConfig := ProjectsConfig{}
	err = yaml.Unmarshal(configFile, &projectsConfig)
	projects := projectsConfig.Projects
	for _, project := range projects {
		queues := project.Queues
		for key, _ := range queues {
			queues[key].project = &project
			allQueues = append(allQueues, &queues[key])
		}
	}
	return allQueues
}
//声明交换器
func (qc *QueueConfig) DeclareExchange(channel *amqp.Channel) {
	//四种性质的交换器
	exchanges := []string{
		qc.WorkerExchangeName(),
		qc.RetryExchangeName(),
		qc.ErrorExchangeName(),
		qc.RequeueExchangeName(),
	}
	//利用通道去声明交换器
	for _, value := range exchanges {
		err := channel.ExchangeDeclare(value,"topic",true, false, false, false, nil)
		if err != nil {
			log.Println("声明交换器失败",err)
			return
		}
	}
	log.Println("声明交换器成功")
}

//工作交换器
func (qc *QueueConfig)WorkerExchangeName() string {
	//如果没有绑定交换器
	if qc.BindingExchange == "" {
		return qc.project.QueuesDefaultConfig.BindingExchange
	}
	return qc.BindingExchange
}

//重试交换器 本队列重试
func (qc *QueueConfig) RetryExchangeName() string {
	return qc.QueueName+"-retry"
}
//错误交换器
func (qc *QueueConfig) ErrorExchangeName() string {
	return qc.QueueName+"-error"
}
//重新放进队列 交换器名称
func (qc *QueueConfig) RequeueExchangeName() string {
	return qc.QueueName+"-retry-requeue"
}

//声明队列
func (qc *QueueConfig) DeclareQueue(channel *amqp.Channel) {
	//工作队列 --> 超时就推进死信队列
	workerQueueOptions := map[string]interface{}{"x-dead-letter-exchange": qc.RetryExchangeName()}
	_,err := channel.QueueDeclare(qc.WorkerQueueName(),true,false,false,false,workerQueueOptions)
	if err != nil {
		log.Println("工作队列声明失败",err)
		return
	}
	for _, value := range qc.RoutingKey {
		err = channel.QueueBind(qc.WorkerQueueName(),value,qc.WorkerExchangeName(),false,nil)
		if err != nil {
			log.Println("工作队列绑定交换器失败",err)
			return
		}
	}
	// 最后，绑定工作队列 和 requeue Exchange
	err = channel.QueueBind(qc.WorkerQueueName(), "#", qc.RequeueExchangeName(), false, nil)
	if err != nil {
		log.Println("将工作队列与重新推进队列的交换器绑定失败",err)
		return
	}

	//重试队列
	retryOptions := map[string]interface{}{
		"x-dead-letter-exchange": qc.RequeueExchangeName(),
		"x-message-ttl":          int32(qc.RetryDurationWithDefault() * 1000)}
	_,err = channel.QueueDeclare(qc.RetryQueueName(),true,false,false,false,retryOptions)
	if err != nil {
		log.Println("重试队列声明失败",err)
		return
	}
	err = channel.QueueBind(qc.RetryQueueName(),"#",qc.RetryExchangeName(),false,nil)
	if err != nil {
		log.Println("重试队列绑定失败",err)
		return
	}
	//错误队列
	_,err = channel.QueueDeclare(qc.ErrorQueueName(),true,false,false,false,nil)
	if err != nil {
		log.Println("错误队列声明失败",err)
		return
	}
	err = channel.QueueBind(qc.ErrorQueueName(),"#",qc.ErrorExchangeName(),false,nil)
	if err != nil {
		log.Println("错误队列绑定失败",err)
		return
	}
}
//工作队列
func (qc *QueueConfig) WorkerQueueName() string {
	return qc.QueueName
}
//重试队列
func (qc *QueueConfig) RetryQueueName() string {
	return qc.QueueName+"-retry"
}
//重试时间
func (qc *QueueConfig) RetryDurationWithDefault() int {
	if qc.RetryDuration == 0 {
		return qc.project.QueuesDefaultConfig.RetryDuration
	}
	return qc.RetryDuration
}
//错误队列
func (qc QueueConfig) ErrorQueueName() string {
	return qc.QueueName+"-error"
}

//默认重试次数
func (qc QueueConfig) RetryTimesWithDefault() int {
	if qc.RetryTimes == 0 {
		return qc.project.QueuesDefaultConfig.RetryTimes
	}
	return qc.RetryTimes
}