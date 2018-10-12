package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"os"
	"strings"
)

var athenaClient *athena.Athena

func init() {
	cred := credentials.NewStaticCredentials(
		os.Getenv("ACCESS_KEY_ID"),
		os.Getenv("SECRET_ACCESS_KEY"),
		"",
	)

	conf := aws.Config{
		Region:      aws.String(os.Getenv("DEFAULT_REGION")),
		Credentials: cred,
	}
	sess := session.New(&conf)
	athenaClient = athena.New(sess)
}

func handler(ctx context.Context, s3Event events.S3Event) {
	for _, record := range s3Event.Records {
		s3 := record.S3
		fmt.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, s3.Bucket.Name, s3.Object.Key)
		keys := strings.Split(s3.Object.Key, "/")
		sql := "ALTER TABLE table_name ADD IF NOT EXISTS PARTITION (dt=" + keys[0] + "-" + keys[1] + "-" + keys[2] + ") "
		sql += "LOCATION 's3://" + s3.Bucket.Name + "/" + keys[0] + "/" + keys[1] + "/" + keys[2] + "/'"
		fmt.Printf(sql)
		input := &athena.StartQueryExecutionInput{
			QueryString: &sql,
		}

		athenaClient.StartQueryExecution(input)

	}
}

func main() {
	lambda.Start(handler)
}
