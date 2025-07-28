package main

import (
	"context"
	"fmt"
	"kafka-test/adaptor/kfkaProducer"
	"kafka-test/entity"
	"time"
)

func main() {

	//Sync producer
	//msg := entity.MsgProcessor{
	//	ID:   1,
	//	Lat:  235.5,
	//	Lon:  -23.5,
	//	City: "London",
	//}
	//kafkaAdaptor := kfkaProducer.New([]string{"localhost:9092"})
	//
	//for {
	//
	//	pErr := kafkaAdaptor.Produce("adsb-raw", msg)
	//	if pErr != nil {
	//		fmt.Println("Error producing message")
	//		continue
	//	}
	//	time.Sleep(5 * time.Second)
	//}

	//Async producer
	//done := make(chan os.Signal, 1)
	//signal.Notify(done, os.Interrupt)
	kafkaAdaptor := kfkaProducer.New([]string{"localhost:9092"})
	defer kafkaAdaptor.Close()
	msgs := []any{
		entity.MsgProcessor{
			ID:   1,
			Lat:  235.5,
			Lon:  -23.5,
			City: "London",
		},
		entity.MsgProcessor{
			ID:   2,
			Lat:  2335.5,
			Lon:  -23.5,
			City: "Tehran",
		},
		entity.MsgProcessor{
			ID:   3,
			Lat:  135.5,
			Lon:  -243.5,
			City: "Paris",
		},
		entity.MsgProcessor{
			ID:   4,
			Lat:  442.5,
			Lon:  -44.5,
			City: "Tokyo",
		},
		entity.MsgProcessor{
			ID:   5,
			Lat:  777.5,
			Lon:  -11.5,
			City: "New York",
		},
		entity.MsgProcessor{
			ID:   6,
			Lat:  99.5,
			Lon:  -15.5,
			City: "Berlin",
		},
		entity.MsgProcessor{
			ID:   7,
			Lat:  567.5,
			Lon:  -98.5,
			City: "Hamburg",
		},
		entity.MsgProcessor{
			ID:   8,
			Lat:  76.5,
			Lon:  -453.5,
			City: "Shiraz",
		},
		entity.MsgProcessor{
			ID:   9,
			Lat:  67.5,
			Lon:  -223.5,
			City: "vienna",
		},
	}
	//apiData := make(chan []any, 10)
	//go func() {
	//	for {
	//		apiData <- msgs
	//		time.Sleep(5 * time.Second)
	//	}
	//}()
	//for messages := range apiData {
	//	fmt.Println("shity", messages)
	//	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Second*3)
	//	kafkaAdaptor.Produce(ctx, cancelCtx, "adsb-raw", messages)
	//}
	count := 0
	for range 5 {
		count++
		fmt.Println("count", count)
		ctx, cancelCtx := context.WithTimeout(context.Background(), time.Second*3)
		kafkaAdaptor.Produce(ctx, cancelCtx, "adsb-raw", msgs)
		time.Sleep(time.Second * 6)
	}
}
