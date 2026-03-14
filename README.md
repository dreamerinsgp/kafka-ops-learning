# Kafka Ops Learning

Go 项目，用于学习常见 Kafka 运维场景。每个问题提供工具查看、连接测试、消费延迟等能力。

## 环境要求

- Go 1.21+
- Kafka（本地或云，如阿里云 Kafka）

## 配置

1. 复制 `.env.example` 为 `.env`
2. 填写 `KAFKA_BROKERS`（host:port，逗号分隔）、`KAFKA_USERNAME`、`KAFKA_PASSWORD`（SASL 时）
3. 加载：`source .env` 或 `export $(cat .env | xargs)`
4. 公网接入需配置 SASL；VPC 内网 9092 通常 PLAINTEXT

## 用法

```bash
# 连接测试
go run ./cmd run 03-connection-failure test

# 列出 Topic
go run ./cmd run 04-topic-missing list

# 消费延迟 Lag
go run ./cmd run 01-consumer-lag info
go run ./cmd run 01-consumer-lag describe

# 分区重平衡 Group 状态
go run ./cmd run 02-rebalance info

# 消息顺序说明
go run ./cmd run 05-message-ordering info
```

## 结构

```
kafka-ops-learning/
├── cmd/main.go           # CLI 入口
├── pkg/kafka/            # 共享 Kafka 客户端（KAFKA_BROKERS, SASL）
└── problems/
    ├── consumerlag/      # 01-consumer-lag: Lag、Topic/Group 描述
    ├── rebalance/        # 02-rebalance: Group 状态
    ├── connection/       # 03-connection-failure: 连接测试
    ├── topicmissing/    # 04-topic-missing: 列出 Topic
    └── messageordering/  # 05-message-ordering: 分区与 key 说明
```

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| KAFKA_BROKERS | host:port，逗号分隔 | 127.0.0.1:9092 |
| KAFKA_USERNAME | SASL 用户名（公网 9093 需填） | - |
| KAFKA_PASSWORD | SASL 密码 | - |
| KAFKA_GROUP | 可选，指定 Consumer Group 过滤 | - |
| KAFKA_TOPIC | 可选，指定 Topic 过滤 | - |
# kafka-ops-learning
