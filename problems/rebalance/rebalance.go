package rebalance

import (
	"fmt"
	"log"
	"os"

	"kafka-ops-learning/pkg/kafka"
)

// Run executes the rebalance info. Action: info
func Run(action string) {
	if action != "info" {
		log.Fatalf("Unknown action: %s (use info)", action)
	}

	admin, err := kafka.NewClusterAdmin()
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer admin.Close()

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		log.Fatalf("列出 Consumer Group 失败: %v", err)
	}

	fmt.Println("Consumer Group 列表（Rebalance 时 Group 会重新分配分区）:")
	if len(groups) == 0 {
		fmt.Println("  (无)")
		return
	}

	groupFilter := os.Getenv("KAFKA_GROUP")
	for name := range groups {
		if groupFilter != "" && name != groupFilter {
			continue
		}
		desc, err := admin.DescribeConsumerGroups([]string{name})
		if err != nil {
			fmt.Printf("  %s: 描述失败 %v\n", name, err)
			continue
		}
		if len(desc) == 0 {
			continue
		}
		d := desc[0]
		state := "Unknown"
		if d.State != "" {
			state = d.State
		}
		fmt.Printf("  %s (状态: %s, 成员: %d)\n", name, state, len(d.Members))
	}
}
