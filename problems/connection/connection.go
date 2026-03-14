package connection

import (
	"fmt"
	"log"

	"kafka-ops-learning/pkg/kafka"
)

// Run executes the connection test. Action: test
func Run(action string) {
	if action != "test" {
		log.Fatalf("Unknown action: %s (use test)", action)
	}

	client, err := kafka.NewClient()
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer client.Close()

	brokers := client.Brokers()
	fmt.Println("✓ Kafka 连接成功")
	fmt.Printf("Brokers: %d 个\n", len(brokers))
	for _, b := range brokers {
		connected, _ := b.Connected()
		if connected {
			fmt.Printf("  - %s (已连接)\n", b.Addr())
		} else {
			fmt.Printf("  - %s (未连接)\n", b.Addr())
		}
	}
}
