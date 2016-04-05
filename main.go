package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/tylertreat/NSQ/benchmark"
	"github.com/tylertreat/NSQ/benchmark/mq"
)

func newTester(subject string, testLatency bool, msgCount, msgSize int, channel string) *benchmark.Tester {
	var messageSender benchmark.MessageSender
	var messageReceiver benchmark.MessageReceiver

	switch subject {
	case "nsq":
		nsq := mq.NewNsq(msgCount, testLatency, channel)
		messageSender = nsq
		messageReceiver = nsq
	default:
		return nil
	}

	return &benchmark.Tester{
		subject,
		msgSize,
		msgCount,
		testLatency,
		messageSender,
		messageReceiver,
	}
}

func parseArgs(usage string) (string, bool, int, int) {

	if len(os.Args) < 2 {
		log.Print(usage)
		os.Exit(1)
	}

	test := os.Args[1]
	messageCount := 1000000
	messageSize := 1000
	testLatency := false

	if len(os.Args) > 2 {
		latency, err := strconv.ParseBool(os.Args[2])
		if err != nil {
			log.Print(usage)
			os.Exit(1)
		}
		testLatency = latency
	}

	if len(os.Args) > 3 {
		count, err := strconv.Atoi(os.Args[3])
		if err != nil {
			log.Print(usage)
			os.Exit(1)
		}
		messageCount = count
	}

	if len(os.Args) > 4 {
		size, err := strconv.Atoi(os.Args[4])
		if err != nil {
			log.Print(usage)
			os.Exit(1)
		}
		messageSize = size
	}

	return test, testLatency, messageCount, messageSize
}

func main() {
	usage := fmt.Sprintf(
		"usage: %s "+
			"{"+
			"inproc|"+
			"zeromq|"+
			"nanomsg|"+
			"kestrel|"+
			"kafka|"+
			"rabbitmq|"+
			"nsq|"+
			"redis|"+
			"activemq|"+
			"nats|"+
			"beanstalkd|"+
			"iris"+
			"} "+
			"[test_latency] [num_messages] [message_size]",
		os.Args[0])

	for i:=0; i<256; i++ {
		tester := newTester("nsq", true, 10000, 1024, strconv.Itoa(i)) //parseArgs(usage)
		if tester == nil {
			log.Println(usage)
			os.Exit(1)
		}

		go tester.Test()
	}
	for {
		time.Sleep(50 * time.Second)
	}
}


