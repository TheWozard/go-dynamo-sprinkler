package table

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Delete uses the dynamodb.DynamoDB and a config to clean up previous table
func Delete(ctx context.Context, svc *dynamodb.DynamoDB, config Config) (*dynamodb.DeleteTableOutput, error) {
	return svc.DeleteTableWithContext(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(config.TableName),
	})
}
