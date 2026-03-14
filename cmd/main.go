package main

import (
	"fmt"
	"os"

	"kafka-ops-learning/problems/connection"
	"kafka-ops-learning/problems/consumerlag"
	"kafka-ops-learning/problems/messageordering"
	"kafka-ops-learning/problems/rebalance"
	"kafka-ops-learning/problems/topicmissing"
)

func main() {
	if len(os.Args) < 3 {
		printUsage()
		os.Exit(1)
	}
	subcmd := os.Args[1]
	problem := os.Args[2]
	action := ""
	if len(os.Args) >= 4 {
		action = os.Args[3]
	}

	if subcmd != "run" {
		printUsage()
		os.Exit(1)
	}

	switch problem {
	case "01-consumer-lag":
		consumerlag.Run(action)
	case "02-rebalance":
		rebalance.Run(action)
	case "03-connection-failure":
		connection.Run(action)
	case "04-topic-missing":
		topicmissing.Run(action)
	case "05-message-ordering":
		messageordering.Run(action)
	default:
		fmt.Printf("Unknown problem: %s\n", problem)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage: go run ./cmd run <problem> [action]

Problems and actions:
  01-consumer-lag       info | describe
  02-rebalance          info
  03-connection-failure test
  04-topic-missing      list
  05-message-ordering   info

Set env:
  KAFKA_BROKERS=host:9092[,host2:9092]   (default 127.0.0.1:9092)
  KAFKA_USERNAME=                        (optional, for SASL)
  KAFKA_PASSWORD=                        (optional)
  KAFKA_GROUP=                           (optional, for consumer lag)
  KAFKA_TOPIC=                           (optional, for describe/lag)`)
}
