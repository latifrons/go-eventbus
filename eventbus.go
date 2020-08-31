package eventbus

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

var ErrNotSupported = errors.New("not supported")

type Subscriber interface {
	Name() string
	Receive(topic int, msg interface{}) error
}

type EventBus struct {
	TimeoutControl bool
	Timeout        time.Duration
	subscribers    map[int][]Subscriber
	eventTypes     map[int]string
	mu             sync.RWMutex
}

func NewEventBus(timeoutControl bool, timeout time.Duration) *EventBus {
	return &EventBus{
		TimeoutControl: timeoutControl,
		Timeout:        timeout,
		subscribers:    make(map[int][]Subscriber),
		eventTypes:     make(map[int]string),
	}
}

func (e *EventBus) RegisterEventType(topic int, topicName string) {
	if e == nil {
		// allow empty eventbus for those modules that doesn't need an eventbus
		return
	}
	if _, ok := e.eventTypes[topic]; ok {
		logrus.WithField("topic", topic).Fatal("topic already exists. programmer bug")
	}
	e.eventTypes[topic] = topicName
}

func (e *EventBus) Subscribe(topic int, subscriber Subscriber) {
	if e == nil {
		// allow empty eventbus for those modules that doesn't need an eventbus
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	v, ok := e.subscribers[topic]
	if !ok {
		v = []Subscriber{}
		e.subscribers[topic] = v
	}
	v = append(v, subscriber)
	e.subscribers[topic] = v
}

func (e *EventBus) Publish(topic int, msg interface{}) {
	if e == nil {
		// allow empty eventbus for those modules that doesn't need an eventbus
		return
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	subscribers, ok := e.subscribers[topic]
	if !ok {
		logrus.WithField("topic", topic).Fatal("topic not registered. programmer bug")
	}
	for _, subscriber := range subscribers {
		if e.TimeoutControl {
			b := make(chan struct{})
			go func(subscriber2 Subscriber, finishChan chan struct{}) {
				err := subscriber2.Receive(topic, msg)
				if err != nil {
					logrus.
						WithField("topic", fmt.Sprintf("%d:%s", topic, e.eventTypes[topic])).
						WithError(err).Warn("topic not registered. programmer bug")

				}
				close(b)
			}(subscriber, b)
			select {
			case <-b:
				continue
			case <-time.After(e.Timeout):
				logrus.WithField("sub", subscriber.Name()).
					WithField("topic", fmt.Sprintf("%d:%s", topic, e.eventTypes[topic])).
					Warn("eventbus timeout")
			}
		} else {
			subscriber.Receive(topic, msg)
		}
	}
}
