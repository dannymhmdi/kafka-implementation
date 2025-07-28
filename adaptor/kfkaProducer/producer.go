package kfkaProducer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"sync"
)

//type KafkaAdaptor struct {
//	producer sarama.SyncProducer
//}
//
////SyncProducer
//
//func New(brokers []string) *KafkaAdaptor {
//	// Configuration
//	config := sarama.NewConfig()
//	config.Version = sarama.V2_1_0_0
//	config.Producer.Return.Successes = true
//	config.Producer.RequiredAcks = sarama.WaitForAll
//	config.Producer.Retry.Max = 5
//	producer, err := sarama.NewSyncProducer(brokers, config)
//	if err != nil {
//		log.Fatal("NewSyncProducer:", err)
//	}
//
//	return &KafkaAdaptor{
//		producer: producer,
//	}
//
//}
//
//func (p *KafkaAdaptor) Produce(topic string, msg interface{}) error {
//
//	marshalMsg, mErr := json.Marshal(msg)
//	if mErr != nil {
//		fmt.Println("failed to marshall msg", mErr)
//	}
//	encodeMsg := base64.StdEncoding.EncodeToString(marshalMsg)
//	message := &sarama.ProducerMessage{
//		Topic: topic,
//		Value: sarama.StringEncoder(encodeMsg),
//	}
//
//	// Send message
//	partition, offset, err := p.producer.SendMessage(message)
//	if err != nil {
//		log.Printf("Failed to send message: %v", err)
//		return err
//	} else {
//		fmt.Printf("Sent message to partition %d at offset %d\n", partition, offset)
//	}
//	return nil
//
//}

//AsyncProducer for higher throughput

type KafkaAdaptor struct {
	producer sarama.AsyncProducer
}

func New(brokers []string) *KafkaAdaptor {
	// Configuration
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	producer, err := sarama.NewAsyncProducer(brokers, config)

	if err != nil {
		log.Fatal("failed to make new asyncProducer:", err)
	}

	return &KafkaAdaptor{
		producer: producer,
	}

}

func (p *KafkaAdaptor) Produce(ctx context.Context, cancelCTx context.CancelFunc, topic string, messages []interface{}) {
	var wg sync.WaitGroup
	defer cancelCTx()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range messages {
			select {
			//in this part we understand they produce successfully or not?
			case msg := <-p.producer.Successes():
				fmt.Println("msg produced successfully", *msg)

			case err := <-p.producer.Errors():
				fmt.Println("failed to produce message", err)
			case <-ctx.Done():
				return
			}
		}
	}()

	for _, msg := range messages {
		marshalMsg, mErr := json.Marshal(msg)
		if mErr != nil {
			fmt.Println("failed to marshall msg", mErr)
			continue
		}
		encodeMsg := base64.StdEncoding.EncodeToString(marshalMsg)

		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(encodeMsg),
		}
		//in this part case success we understand they send successfully or not?

		select {
		//in this part just send messages to input or queue
		case p.producer.Input() <- message:
			fmt.Println("put messages into input or queue to produce them")
		case <-ctx.Done():
			return
		}

	}
	wg.Wait()
}

func (ka *KafkaAdaptor) Close() {
	ka.producer.Close()
}

func GetUnconsumedMessages(brokers []string, topic, group string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0 // Match your Kafka version

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Fatalf("Failed to create cluster admin: %v", err)
	}
	defer admin.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	var totalUnconsumed int64

	for _, partition := range partitions {
		latestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("Failed to get latest offset: %v", err)
		}

		// Get the committed offset of the group
		offsetFetchRequest := &sarama.OffsetFetchRequest{
			Version:       1,
			ConsumerGroup: group,
		}
		offsetFetchRequest.AddPartition(topic, partition)

		broker, err := client.Coordinator(group)
		if err != nil {
			log.Fatalf("Failed to get coordinator: %v", err)
		}

		offsetFetchResponse, err := broker.FetchOffset(offsetFetchRequest)
		if err != nil {
			log.Fatalf("Failed to fetch offset: %v", err)
		}

		block := offsetFetchResponse.GetBlock(topic, partition)
		if block == nil || block.Err != sarama.ErrNoError {
			log.Fatalf("Failed to fetch committed offset for partition %d: %v", partition, block.Err)
		}

		committedOffset := block.Offset
		unconsumed := latestOffset - committedOffset
		fmt.Printf("Partition %d => Committed: %d, Latest: %d, Pending: %d\n",
			partition, committedOffset, latestOffset, unconsumed)
		totalUnconsumed += unconsumed
	}

	fmt.Printf("🔄 Total unconsumed messages in topic '%s' for group '%s': %d\n", topic, group, totalUnconsumed)
}
