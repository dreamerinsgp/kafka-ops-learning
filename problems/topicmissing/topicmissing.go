package topicmissing

import (
	"fmt"
	"log"
	"sort"

	"kafka-ops-learning/pkg/kafka"
)

// Run executes the topic list. Action: list
func Run(action string) {
	if action != "list" {
		log.Fatalf("Unknown action: %s (use list)", action)
	}

	client, err := kafka.NewClient()
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer client.Close()

	topics, err := client.Topics()
	if err != nil {
		log.Fatalf("获取 Topic 列表失败: %v", err)
	}

	sort.Strings(topics)
	fmt.Println("Topic 列表:")
	if len(topics) == 0 {
		fmt.Println("  (无)")
		return
	}
	for _, t := range topics {
		parts, _ := client.Partitions(t)
		fmt.Printf("  %s (分区数: %d)\n", t, len(parts))
	}
}
