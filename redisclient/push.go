package redisclient

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
)

var client *redis.Client

const (
	key = "fromKafka"
)

func Init(addr, listKey *string) {
	client = redis.NewClient(&redis.Options{
		Addr:     *addr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	if _, err := client.Ping().Result(); err != nil {
		handleErr(err)
	}
}

func LPush(val string){
	if _, err := client.LPush(key, val).Result(); err != nil {
		handleErr(err)
	}
}

func RPush(val string){
	if _, err := client.LPush(key, val).Result(); err != nil {
		handleErr(err)
	}
}

func LRange() int64 {
	llen, err := client.LLen(key).Result()
	if err != nil {
		panic(err.Error())
	}
	values, err2 := client.LRange(key, 0, -1).Result()
	if err2 != nil {
		handleErr(err2)
	}
	for _, val := range values {
		fmt.Println(val)
	}
	return llen
}

func handleErr(err error) {
	log.Print(err.Error())
	panic(err.Error())
}
