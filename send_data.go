package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"golang.org/x/time/rate"
)

type User struct {
	UserId       string  `json:"userId"`
	LastUpdate   int64   `json:"lastUpdate"`
	Score        int32   `json:"score"`
	partitionKey *string `json:"-"`
}

const UserCount = 100000

const UserBatchSize = 500

const SendDebugCount = 1000

const RequestsPerSecond = 990

var KinesisStreamName = aws.String("leaderboard")

func main() {

	awsSession := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))

	svc := kinesis.New(awsSession)
	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Initialize the users
	users := make([]*User, UserCount)
	for idx := 0; idx < UserCount; idx++ {
		users[idx] = &User{UserId: "user-" + strconv.Itoa(idx), Score: 0, partitionKey: aws.String(strconv.Itoa(random.Int()))}
	}

	limiter := rate.NewLimiter(RequestsPerSecond, RequestsPerSecond)

	count := 0
	var userBatch []*User

	for {

		for _, user := range users {

			userBatch = append(userBatch, user)

			if len(userBatch) >= UserBatchSize {
				if canSend(limiter, len(userBatch)) {
					sendUpdates(svc, userBatch)
					count += len(userBatch)
					if count%SendDebugCount == 0 {
						fmt.Println("Count: " + strconv.Itoa(count))
					}
					userBatch = nil
				}
			}
		}
	}
}

func canSend(limiter *rate.Limiter, count int) bool {
	now := time.Now()
	reservation := limiter.ReserveN(now, count)

	if !reservation.OK() {
		fmt.Println("Too much")
		time.Sleep(100 * time.Microsecond)
		return false
	}

	delay := reservation.DelayFrom(now)

	if delay > 0 {
		time.Sleep(delay)
	}

	return true
}

func sendUpdates(kinesisClient *kinesis.Kinesis, users []*User) {

	var records []*kinesis.PutRecordsRequestEntry

	for _, user := range users {
		user.LastUpdate = currentTimeInMillis()
		user.Score += rand.Int31n(100)

		userEventJson, err := json.Marshal(user)
		if err != nil {
			fmt.Println(err)
			continue
		}

		records = append(records, &kinesis.PutRecordsRequestEntry{Data: userEventJson, PartitionKey: user.partitionKey})
	}

	_, err := kinesisClient.PutRecords(&kinesis.PutRecordsInput{
		Records:    records,
		StreamName: KinesisStreamName,
	})

	if err != nil {
		fmt.Println(err)
	}

}

func currentTimeInMillis() int64 {
	tv := new(syscall.Timeval)
	syscall.Gettimeofday(tv)
	return (int64(tv.Sec)*1e3 + int64(tv.Usec)/1e3)
}
