### if you want produce message and consume by multi-services should use different groupid for 
each service . see implementation in this repo

### 🧩 Partition in Kafka
- Partition is a subset of a Kafka topic.

- Each topic is split into one or more partitions.

- Partitions allow Kafka to scale horizontally by distributing data and load across multiple brokers.

- Each partition is an ordered, immutable sequence of messages.

- Partitions enable parallel processing because different consumers in a consumer group can read from different partitions simultaneously.


### Why partitions matter:
- They increase throughput by allowing messages to be spread across brokers.

- They maintain message order within a partition (but not across partitions).

- They provide fault tolerance through replication.

### notice:
- Each consumer in a consumer group is assigned one or more unique partitions of a topic.

- No two consumers in the same consumer group will consume the same partition simultaneously.

- This ensures each message in a partition is processed by exactly one consumer in that group, enabling parallelism without duplication.


| Concept                         | Explanation                                                            |
| ------------------------------- | ---------------------------------------------------------------------- |
| **Consumer Group**              | A set of consumers sharing the workload of a topic.                    |
| **Partition Assignment**        | Kafka assigns each partition to only one consumer in the group.        |
| **If #Consumers > #Partitions** | Some consumers will be idle because partitions can't be split further. |
| **If #Partitions > #Consumers** | Some consumers will consume multiple partitions.                       |


### What happens when you consume with 2 different group IDs?
- Each group ID represents a separate consumer group.

- Kafka treats each consumer group independently.

- Each group will receive every message from the topic, regardless of what other groups do.

- This is exactly how you enable multiple independent services to consume the same topic messages without interfering.
- So group-A and group-B each consume all partitions independently.

- If your topic has 3 partitions (P0, P1, P2), each group will receive every message from P0–P2.

### Question:so messages spread in to different partitions of a topic?

Messages in Kafka are spread (distributed) across different partitions of a topic — not duplicated.


## kafka consumer:

```go
package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"msgprocessor-service/contract/types"
)

type ConsumerGroup struct {
	HandlerFunc   types.KafkaHandler
	consumerGroup sarama.ConsumerGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

func New(brokers []string, groupId string, handler types.KafkaHandler) *ConsumerGroup {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupId, config)
	if err != nil {
		log.Fatal("failed to create consumer group:", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ConsumerGroup{
		HandlerFunc:   handler,
		consumerGroup: consumerGroup,
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (h *ConsumerGroup) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerGroup) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerGroup) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	//for msg := range claim.Messages() {
	//	if hErr := h.HandlerFunc(msg); hErr == nil {
	//		sess.MarkMessage(msg, "")
	//	} else {
	//		fmt.Println("failed to process msg", msg, hErr)
	//	}
	//}
	//return nil

	for msg := range claim.Messages() {
		select {
		case <-h.ctx.Done():
			return nil
		default:
			if err := h.HandlerFunc(msg); err != nil {
				fmt.Println("failed to process message:", err)
				continue
			}

			sess.MarkMessage(msg, "")
			fmt.Printf("Processed message (topic=%s, partition=%d, offset=%d,key=%s)\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Key))
		}
	}

	return nil
}

func (cg *ConsumerGroup) Close() error {
	cg.cancel()
	return cg.consumerGroup.Close()
}

```

```go
package kafka

import (
	"fmt"
	"time"
)

func (cg *ConsumerGroup) Consume(topics []string) {
	// Joins the Kafka consumer group
	//Gets assigned partitions (from the topics)
	//️ Triggers:
	//Setup()
	//Then for each assigned partition: ConsumeClaim()
	// Exits when:
	//Rebalance happens (another consumer joins or leaves the group)
	//An error occurs
	//Context is cancelled

	for {
		select {
		case <-cg.ctx.Done():
			return
		default:
			err := cg.consumerGroup.Consume(cg.ctx, topics, cg)
			if err != nil {
				fmt.Println("failed to join consumer group and assign partition to that consumerGroup:", err)
				time.Sleep(2 * time.Second) // prevent busy looping on errors
			}
		}
	}

}

```

### notice :

- You can produce message in topic and consume it by 2 different consumer group but each consumer should have different groupid
- Each consumer group can have multi consumer , consumers with same groupid are in a same consumer group
if we have multi consumers in one consumer group can each partition assigns to each consumer in group 
- Also if number of consumers is more than partition some consumers id idle
- To have multi consumer in one consumer group just need to have same group id like below:
each consumer can consume each partion of specific topic , for example in below code if "my-topic" has 2 
partition each consumer in consumer group with groupid "my-group" take one partition to consume messages
```go

// Create multiple consumers with the SAME group ID
consumer1, err := kafka.New(brokers, "my-group", handler)
consumer2, err := kafka.New(brokers, "my-group", handler)

// Start consuming the same topics
go consumer1.Consume([]string{"my-topic"})
go consumer2.Consume([]string{"my-topic"})

```
- If we want to produce message in topic and multi services interest that topic we need 2 consumers with different group id 
now each consumer group (consumerA with "airnav" & consumerB with "flight24" group-id) can have multi consumers in their group 
if create consumers with same group id like above code


```go
func main() {
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt)
	brokers := []string{"localhost:9092"}
	consumerA := kafka.New(brokers, "airnav", AirNavHandler)
	consumerB := kafka.New(brokers, "flight24", Flight24Handler)

	go consumerA.Consume([]string{"live-flight-airnav"})
	go consumerB.Consume([]string{"live-flight-flight24"})
	<-done

}
```

in below function when use sarama package :

```go
func (cg *ConsumerGroup) Consume(topics []string) {
	// Joins the Kafka consumer group
	//Gets assigned partitions (from the topics)
	//️ Triggers:
	//Setup()
	//Then for each assigned partition: ConsumeClaim()
	// Exits when:
	//Rebalance happens (another consumer joins or leaves the group)
	//An error occurs
	//Context is cancelled

	for {
		select {
		case <-cg.ctx.Done():
			return
		default:
			err := cg.consumerGroup.Consume(cg.ctx, topics, cg)
			if err != nil {
				fmt.Println("failed to join consumer group and assign partition to that consumerGroup:", err)
				time.Sleep(2 * time.Second) // prevent busy looping on errors
			}
		}
	}

}
```
first join to consumer group then assigns partition of that topic to consumer in that consumer group:
```go
consumer1, err := kafka.New(brokers, "my-group", handler)
consumer2, err := kafka.New(brokers, "my-group", handler)
```
Both consumer1 and consumer2 are in same group ,so they join to their consumer group and assigns partion of that topic , because they both are in a same group so they consume same topic .
