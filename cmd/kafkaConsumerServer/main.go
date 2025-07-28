package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"kafka-test/adaptor/kfkaConsumer"
	"kafka-test/entity"
)

func main() {
	brokers := []string{"localhost:9092"}
	groupID := "groupID11"
	kafkaAdaptor := kfkaConsumer.New(brokers, groupID, func(msg *sarama.ConsumerMessage) error {
		decodeString, dErr := base64.StdEncoding.DecodeString(string(msg.Value))
		if dErr != nil {
			fmt.Println("failed to start kafka consumer'")
			return dErr
		}
		var message entity.MsgProcessor
		if mErr := json.Unmarshal(decodeString, &message); mErr != nil {
			fmt.Println("failed to unmarshall msg", mErr)

			return mErr
		}
		fmt.Println("msg-server-1:", message)
		return nil
	})

	kafkaAdaptor.Consume([]string{"adsb-raw"})
}
