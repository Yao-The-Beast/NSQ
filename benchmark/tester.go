package benchmark

//import "log"
import "runtime"

type Tester struct {
	Name         string
	MessageSize  int
	MessageCount int
	TestLatency  bool
	MessageSender
	MessageReceiver
}

func (tester Tester) Test(id int) {
	//log.Printf("Begin %s test", tester.Name)
	if id == 0 {
		runtime.LockOSThread()
	}
	tester.Setup()
	defer tester.Teardown()

	if tester.TestLatency {
		tester.testLatency()
	} else {
		tester.testThroughput()
	}

	//log.Printf("End %s test", tester.Name)
}

func (tester Tester) testThroughput() {
	receiver := NewReceiveEndpoint(tester, tester.MessageCount)
//	sender := &SendEndpoint{MessageSender: tester}
//	sender.TestThroughput(tester.MessageSize, tester.MessageCount)
	receiver.WaitForCompletion()
}

func (tester Tester) testLatency() {
	receiver := NewReceiveEndpoint(tester, tester.MessageCount)
	//sender := &SendEndpoint{MessageSender: tester}
	//sender.TestLatency(tester.MessageSize, tester.MessageCount)
	receiver.WaitForCompletion()
}
