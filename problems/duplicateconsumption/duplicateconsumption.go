package duplicateconsumption

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/IBM/sarama"
	"kafka-ops-learning/pkg/kafka"
)

// Run executes duplicate consumption related actions. Action: info | analyze
func Run(action string) {
	switch action {
	case "info":
		showInfo()
	case "analyze":
		analyzeDuplicate()
	default:
		log.Fatalf("Unknown action: %s (use info or analyze)", action)
	}
}

func showInfo() {
	fmt.Println("=== 消息重复消费：概念与原因 ===")
	fmt.Println()
	fmt.Println("【什么导致重复消费？】")
	fmt.Println("1. Consumer 在处理消息后、提交 Offset 前崩溃")
	fmt.Println("   → 重启后重新消费同一条消息 (At-least-once)")
	fmt.Println()
	fmt.Println("2. Rebalance 期间消费者被踢出 Group")
	fmt.Println("   → 分区重新分配后，原有已处理消息可能被重新消费")
	fmt.Println()
	fmt.Println("3. 手动提交 Offset 但提交失败")
	fmt.Println("   → 误以为已提交，实际未提交，重启后重复")
	fmt.Println()
	fmt.Println("4. 网络抖动导致请求超时")
	fmt.Println("   → Consumer 认为处理失败，可能重试")
	fmt.Println()
	fmt.Println("【关键配置】")
	fmt.Println("- enable.auto.commit: true (自动提交，可能丢消息)")
	fmt.Println("- enable.auto.commit: false (手动提交，需自行处理重复)")
	fmt.Println("- auto.commit.interval.ms: 自动提交间隔")
	fmt.Println("- max.poll.records: 每次拉取最大消息数")
	fmt.Println("- session.timeout.ms: Consumer Group 心跳超时")
	fmt.Println()
	fmt.Println("【如何避免？】")
	fmt.Println("1. 幂等性处理：业务层做去重（如数据库唯一键）")
	fmt.Println("2. 精确一次语义：事务 + 精确一次幂等")
	fmt.Println("3. 合理设置提交间隔：别太长，也别太短")
	fmt.Println("4. 监控重复消费指标")
}

func analyzeDuplicate() {
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

	groupFilter := os.Getenv("KAFKA_GROUP")
	topicFilter := os.Getenv("KAFKA_TOPIC")

	fmt.Println("=== Consumer Group Offset 分析 ===")
	fmt.Println()

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		log.Fatalf("列出 Consumer Group 失败: %v", err)
	}

	if len(groups) == 0 {
		fmt.Println("无 Consumer Group")
		return
	}

	var groupNames []string
	for name := range groups {
		if groupFilter != "" && name != groupFilter {
			continue
		}
		groupNames = append(groupNames, name)
	}
	sort.Strings(groupNames)

	for _, groupName := range groupNames {
		desc, err := admin.DescribeConsumerGroups([]string{groupName})
		if err != nil || len(desc) == 0 {
			continue
		}
		d := desc[0]
		fmt.Printf("Group: %s (状态: %s, 成员数: %d)\n", groupName, d.State, len(d.Members))

		// 获取 Offset 信息
		resp, err := admin.ListConsumerGroupOffsets(groupName, nil)
		if err != nil {
			fmt.Printf("  获取 Offset 失败: %v\n", err)
			continue
		}
		if resp == nil || resp.Blocks == nil {
			continue
		}

		fmt.Println("  分区 Offset 详情:")
		for topic, parts := range resp.Blocks {
			if topicFilter != "" && topic != topicFilter {
				continue
			}
			for partition, block := range parts {
				hw, _ := client.GetOffset(topic, partition, sarama.OffsetNewest)
				// consumer_offset 表示已提交的消费位置
				// 如果 hw - block.Offset 很大，说明还有消息未消费
				// 如果多次看到相同 offset，说明可能重复消费
				lag := hw - block.Offset
				if lag < 0 {
					lag = 0
				}
				fmt.Printf("    [%s-%d] committed_offset=%d, high_watermark=%d, lag=%d\n",
					topic, partition, block.Offset, hw, lag)
			}
		}
		fmt.Println()
	}

	fmt.Println("=== __consumer_offsets 分析 ===")
	fmt.Println("__consumer_offsets 是 Kafka 内部存储 Consumer Group Offset 的 Topic")
	fmt.Println("可通过检查该 Topic 确认 Offset 提交状态")
	fmt.Println()

	// 尝试获取 __consumer_offsets 信息
	coClient, err := sarama.NewClient(BrokersFromEnv(), nil)
	if err != nil {
		log.Printf("无法创建 client: %v", err)
		return
	}
	defer coClient.Close()

	topics, _ := coClient.Topics()
	hasOffsets := false
	for _, t := range topics {
		if t == "__consumer_offsets" {
			hasOffsets = true
			parts, _ := coClient.Partitions(t)
			fmt.Printf("__consumer_offsets: %d 个分区\n", len(parts))
			break
		}
	}
	if !hasOffsets {
		fmt.Println("__consumer_offsets: 未找到 (可能是新版本 Kafka 或配置问题)")
	}
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
