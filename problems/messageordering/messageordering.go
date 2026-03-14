package messageordering

import (
	"fmt"
	"log"

	"kafka-ops-learning/pkg/kafka"
)

// Run prints explanation of partition and key. Action: info
func Run(action string) {
	if action != "info" {
		log.Fatalf("Unknown action: %s (use info)", action)
	}

	client, err := kafka.NewClient()
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer client.Close()

	fmt.Println("=== 消息顺序与分区 ===\n")
	fmt.Println("1. 同一分区内消息有序，分区间无序")
	fmt.Println("2. 相同 key 的消息会进入同一分区（hash(key) % partition_count）")
	fmt.Println("3. 要保证同一业务实体（如订单）的顺序，应以业务 ID 为 key")
	fmt.Println("4. 单分区可保证全局顺序，但吞吐受限\n")

	topics, _ := client.Topics()
	if len(topics) > 0 {
		fmt.Println("当前 Topic 示例（分区数决定最大并行消费者数）:")
		for _, t := range topics {
			parts, _ := client.Partitions(t)
			fmt.Printf("  %s: %d 分区\n", t, len(parts))
		}
	}
}
