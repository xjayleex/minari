package redis

type ConsumerConfig struct {
	NewConsumer bool `config:`
}

type TriggerConsumer struct {
	Trigger bool `config: `
}
