package consumerlag

import (
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/IBM/sarama"
	"kafka-ops-learning/pkg/kafka"
)

// Run executes consumer lag info. Action: info | describe
func Run(action string) {
	switch action {
	case "info":
		showLag()
	case "describe":
		showDescribe()
	default:
		log.Fatalf("Unknown action: %s (use info or describe)", action)
	}
}

func showLag() {
	admin, err := kafka.NewClusterAdmin()
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer admin.Close()

	client, err := kafka.NewClient()
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer client.Close()

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		log.Fatalf("列出 Consumer Group 失败: %v", err)
	}

	topicFilter := os.Getenv("KAFKA_TOPIC")
	groupFilter := os.Getenv("KAFKA_GROUP")

	fmt.Println("Consumer Group Lag (消费者 offset 与分区高水位的差值):")
	if len(groups) == 0 {
		fmt.Println("  (无 Consumer Group)")
		return
	}

	for name := range groups {
		if groupFilter != "" && name != groupFilter {
			continue
		}
		resp, err := admin.ListConsumerGroupOffsets(name, nil)
		if err != nil {
			fmt.Printf("  %s: 获取 offset 失败 %v\n", name, err)
			continue
		}
		if resp == nil || resp.Blocks == nil {
			continue
		}
		var totalLag int64
		hasOutput := false
		for topic, parts := range resp.Blocks {
			if topicFilter != "" && topic != topicFilter {
				continue
			}
			for partition, block := range parts {
				hw, _ := client.GetOffset(topic, partition, sarama.OffsetNewest)
				lag := hw - block.Offset
				if lag < 0 {
					lag = 0
				}
				totalLag += lag
				fmt.Printf("  %s [%s-%d] consumer_offset=%d hw=%d lag=%d\n",
					name, topic, partition, block.Offset, hw, lag)
				hasOutput = true
			}
		}
		if hasOutput {
			fmt.Printf("  %s 总 Lag: %d\n", name, totalLag)
		}
	}
}

func showDescribe() {
	admin, err := kafka.NewClusterAdmin()
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer admin.Close()

	client, err := kafka.NewClient()
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer client.Close()

	fmt.Println("=== Topics ===")
	topics, _ := client.Topics()
	sort.Strings(topics)
	for _, t := range topics {
		parts, _ := client.Partitions(t)
		fmt.Printf("%s: %d 分区\n", t, len(parts))
	}

	fmt.Println("\n=== Consumer Groups ===")
	groups, err := admin.ListConsumerGroups()
	if err != nil {
		log.Printf("列出 Group 失败: %v", err)
		return
	}
	for name := range groups {
		desc, err := admin.DescribeConsumerGroups([]string{name})
		if err != nil || len(desc) == 0 {
			continue
		}
		d := desc[0]
		fmt.Printf("%s (状态: %s, 成员: %d)\n", name, d.State, len(d.Members))
	}
}
