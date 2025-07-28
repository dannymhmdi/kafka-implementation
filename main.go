package main

import "kafka-test/adaptor/kfka"

func main() {
	kfka.GetUnconsumedMessages([]string{"localhost:9092"}, "adsb-raw", "groupID1")
}
