package eventbus

import (
	"fmt"
	"testing"
)

type TestSubscriber struct {
}

func (t TestSubscriber) Name() string {
	return "TestSubscriber"
}

func (t TestSubscriber) Receive(topic int, msg interface{}) error {
	fmt.Printf("%d %+v\n", topic, msg)
	//time.Sleep(time.Second * 10)
	return nil
}

func TestNewEventBus(t *testing.T) {
	eb := NewEventBus(&EventBusConfig{
		TimeoutControl: false,
		Timeout:        0,
		OnPublishFunc:  nil,
	})
	ts := &TestSubscriber{}
	eb.RegisterEventType(1, "T1")
	eb.Subscribe(1, ts)
	eb.Publish(1, "xxx")
}

func TestNilEventBus(t *testing.T) {
	var eb *EventBus
	eb.Publish(1, nil)
}
