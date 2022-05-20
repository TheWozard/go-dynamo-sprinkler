package table

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Create uses the dynamodb.DynamoDB and config to construct a table the matches the format need for the sprinkler
func Create(ctx context.Context, svc *dynamodb.DynamoDB, config Config) (*dynamodb.CreateTableOutput, error) {
	// Base core of the table, all destination are built using Global Secondary Indexes
	input := &dynamodb.CreateTableInput{
		TableName: aws.String(config.TableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(config.Attributes.ID),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String(config.Attributes.Timestamp),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(config.Attributes.ID),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{},
		BillingMode:            aws.String(dynamodb.BillingModePayPerRequest),
	}
	for _, destination := range config.Destinations {
		// The idea with the index is that we can lookup a particular status and sort from oldest to newest.
		// Then when we complete the delivery, switch the status to a new one
		input.AttributeDefinitions = append(input.AttributeDefinitions, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(destination.StatusAttribute),
			AttributeType: aws.String("S"),
		})
		input.GlobalSecondaryIndexes = append(input.GlobalSecondaryIndexes, &dynamodb.GlobalSecondaryIndex{
			IndexName: aws.String(destination.StatusIndex),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String(destination.StatusAttribute),
					KeyType:       aws.String(dynamodb.KeyTypeHash),
				},
				{
					AttributeName: aws.String(config.Attributes.Timestamp),
					KeyType:       aws.String(dynamodb.KeyTypeRange),
				},
			},
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String(dynamodb.ProjectionTypeAll),
			},
		})
	}
	return svc.CreateTable(input)
}
