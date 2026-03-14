package kafka

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// ConfigFromEnv creates sarama Config from KAFKA_BROKERS, KAFKA_USERNAME, KAFKA_PASSWORD.
// KAFKA_BROKERS: comma-separated host:port (e.g. host1:9092,host2:9092)
// KAFKA_USERNAME: SASL username (optional, for Aliyun public 9093)
// KAFKA_PASSWORD: SASL password (optional)
func ConfigFromEnv() (*sarama.Config, error) {
	cfg := sarama.NewConfig()
	cfg.Net.DialTimeout = 10 * time.Second
	cfg.Net.ReadTimeout = 10 * time.Second
	cfg.Net.WriteTimeout = 10 * time.Second
	cfg.Metadata.Retry.Max = 3
	cfg.Admin.Timeout = 15 * time.Second

	username := strings.TrimSpace(os.Getenv("KAFKA_USERNAME"))
	password := os.Getenv("KAFKA_PASSWORD")
	if username != "" || password != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		cfg.Net.SASL.User = username
		cfg.Net.SASL.Password = password
	}

	return cfg, nil
}

// BrokersFromEnv returns broker list from KAFKA_BROKERS (comma-separated).
// Default: 127.0.0.1:9092
func BrokersFromEnv() []string {
	s := os.Getenv("KAFKA_BROKERS")
	if s == "" {
		return []string{"127.0.0.1:9092"}
	}
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	if len(out) == 0 {
		return []string{"127.0.0.1:9092"}
	}
	return out
}

// NewClient creates a sarama Client from env vars.
func NewClient() (sarama.Client, error) {
	cfg, err := ConfigFromEnv()
	if err != nil {
		return nil, err
	}
	brokers := BrokersFromEnv()
	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return client, nil
}

// NewClusterAdmin creates a sarama ClusterAdmin from env vars.
func NewClusterAdmin() (sarama.ClusterAdmin, error) {
	cfg, err := ConfigFromEnv()
	if err != nil {
		return nil, err
	}
	brokers := BrokersFromEnv()
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka admin: %w", err)
	}
	return admin, nil
}
