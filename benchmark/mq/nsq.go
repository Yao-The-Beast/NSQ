package mq

import (
	"strconv"
	"github.com/bitly/go-nsq"
	"github.com/tylertreat/nsq/benchmark"
)

type Nsq struct {
	handler benchmark.MessageHandler
	pub     *nsq.Producer
	sub     *nsq.Consumer
	topic   string
	channel string
        raw_channel string
}

func NewNsq(numberOfMessages int, testLatency bool, channeL string) *Nsq {
	//topic := "0#ephemeral"
	//topic := "0"
	channel := channeL
	channel += "#ephemeral"
	topic := channel	
	pub, _ := nsq.NewProducer("localhost:4150", nsq.NewConfig())
	config :=nsq.NewConfig()
	config.MaxInFlight = 2000
	sub, _ := nsq.NewConsumer(topic, channel, config)
	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
			Channel:	channeL,
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return &Nsq{
		handler: handler,
		pub:     pub,
		sub:     sub,
		topic:   topic,
		channel: channel,
		raw_channel: channeL,
	}
}

func (n *Nsq) Setup() {
	n.sub.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		n.handler.ReceiveMessage(message.Body)
		return nil
	}))
	i, _ := strconv.Atoi(n.raw_channel)
	if i < 1000 {
		n.sub.ConnectToNSQD("localhost:4150")
		//n.sub.ConnectToNSQD("10.145.208.27:4150");
	} else {
		n.sub.ConnectToNSQD("localhost:4150")
	}
}

func (n *Nsq) Teardown() {
	n.sub.Stop()
	n.pub.Stop()
}

func (n *Nsq) Send(message []byte) {
	//n.pub.PublishAsync(n.topic, message, nil)
}

func (n *Nsq) MessageHandler() *benchmark.MessageHandler {
	return &n.handler
}
