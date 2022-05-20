package table

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func Exits(ctx context.Context, svc *dynamodb.DynamoDB, config Config) bool {
	_, err := svc.DescribeTableWithContext(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(config.TableName),
	})
	return err == nil
}
