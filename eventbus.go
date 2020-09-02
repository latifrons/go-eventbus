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

type PublishInfo struct {
	Topic          int
	TopicName      string
	SubscriberName string
}

type EventBus struct {
	TimeoutControl bool
	Timeout        time.Duration
	OnPublishFunc  func(PublishInfo, interface{})
	subscribers    map[int][]Subscriber
	eventTypes     map[int]string
	mu             sync.RWMutex
}

func NewEventBus(timeoutControl bool, timeout time.Duration, onPublishFunc func(PublishInfo, interface{})) *EventBus {
	return &EventBus{
		TimeoutControl: timeoutControl,
		Timeout:        timeout,
		OnPublishFunc:  onPublishFunc,
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
	e.subscribers[topic] = []Subscriber{}
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
		logrus.WithField("topic", topic).Warn("topic not registered")
		return
	}
	v = append(v, subscriber)
	e.subscribers[topic] = v
}

func (e *EventBus) PublishAsync(topic int, msg interface{}) {
	go e.Publish(topic, msg)
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
		logrus.WithField("topic", topic).Warn("topic not registered")
		return
	}
	for _, subscriber := range subscribers {
		if e.TimeoutControl {
			b := make(chan struct{})
			go func(subscriber2 Subscriber, finishChan chan struct{}) {
				e.receiveOne(subscriber2, topic, msg)
				close(finishChan)
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
			e.receiveOne(subscriber, topic, msg)
		}
	}
}

func (e *EventBus) receiveOne(subscriber Subscriber, topic int, msg interface{}) {
	if e.OnPublishFunc != nil {
		e.OnPublishFunc(PublishInfo{
			Topic:          topic,
			TopicName:      e.eventTypes[topic],
			SubscriberName: subscriber.Name(),
		}, msg)
	}

	err := subscriber.Receive(topic, msg)
	if err != nil {
		logrus.
			WithField("topic", fmt.Sprintf("%d:%s", topic, e.eventTypes[topic])).
			WithField("sub", subscriber.Name()).
			WithError(err).Warn("event subscriber failed to receive topic event")
	}
}
