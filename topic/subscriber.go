package topic

// Subscriber is an interface for receiving events asynchronly
type Subscriber interface {
	OnEvent(nextAddress uint64, data []byte) error
}

type voidSubscriber struct{}

type SubscriberFunc func(nextAddress uint64, data []byte) error

func (f SubscriberFunc) OnEvent(nextAddress uint64, data []byte) error {
	return f(nextAddress, data)
}
