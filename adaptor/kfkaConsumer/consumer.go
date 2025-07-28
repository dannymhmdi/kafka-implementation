package kfkaConsumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"time"
)

type KafkaAdaptor struct {
	HandlerFunc   func(message *sarama.ConsumerMessage) error
	consumerGroup sarama.ConsumerGroup
}

func (h *KafkaAdaptor) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *KafkaAdaptor) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *KafkaAdaptor) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if hErr := h.HandlerFunc(msg); hErr == nil {
			sess.MarkMessage(msg, "")
		} else {
			fmt.Println("failed to process msg", msg, hErr)
		}
	}
	return nil
}

func New(brokers []string, groupID string, handler func(msg *sarama.ConsumerMessage) error) *KafkaAdaptor {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatal("failed to create consumer group:", err)
	}

	return &KafkaAdaptor{
		HandlerFunc:   handler,
		consumerGroup: consumerGroup,
	}

}

func (ka *KafkaAdaptor) Consume(topics []string) {
	defer ka.consumerGroup.Close()

	ctx := context.Background()

	for {
		fmt.Println("connect broker")
		//connect to broker and customclaim method consume msgs: first connect to broker ==> then consumes msgs by customclaim method
		if err := ka.consumerGroup.Consume(ctx, topics, ka); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
			//after 2 seconds try to connect to broker again
			time.Sleep(2 * time.Second)
		}
	}

}
