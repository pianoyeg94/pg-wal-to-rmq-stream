package rmq

import "time"

type Config struct {
	ConnectionString  string        `yaml:"connection_string"`
	ConnectionTimeout time.Duration `yaml:"connection_timeout"`
	Heartbeat         time.Duration `yaml:"heartbeat"`
	Exchange          string        `yaml:"exchange"`
	RoutingKey        string        `yaml:"routing_key"`
	Concurrency       int           `yaml:"concurrency"`
	Reconnect         RetryConfig   `yaml:"reconnect"`
	Republish         RetryConfig   `yaml:"republish"`
}

type RetryConfig struct {
	MaxAttempts int           `yaml:"max_attempts"`
	DelayStart  time.Duration `yaml:"delay_start"`
	DelayStep   time.Duration `yaml:"delay_step"`
	DelayStop   time.Duration `yaml:"delay_stop"`
}
