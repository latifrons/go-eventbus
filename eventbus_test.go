package eventbus

import (
	"fmt"
	"testing"
	"time"
)

type TestSubscriber struct {
}

func (t TestSubscriber) Name() string {
	return "TestSubscriber"
}

func (t TestSubscriber) Receive(topic int, msg interface{}) {
	fmt.Printf("%d %+v\n", topic, msg)
	time.Sleep(time.Second * 10)
}

func TestNewEventBus(t *testing.T) {
	eb := NewEventBus(true, time.Second*5)
	ts := &TestSubscriber{}
	eb.RegisterEventType(1, "T1")
	eb.Subscribe(1, ts)
	eb.Publish(1, "xxx")
}