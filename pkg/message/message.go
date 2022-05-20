package message

import (
	"context"
	"time"

	"github.com/TheWozard/go-dynamo-sprinkler/pkg/table"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

const (
	TimestampFormat = time.RFC3339Nano

	StatusReady            = "READY"
	StatusDelivered        = "DELIVERED"
	StatusFailedProvidence = "FAILED-PROVIDENCE"
)

type Message struct {
	ID         string
	Timestamp  time.Time
	Sum        string
	Providence string
	Status     string
}

type Receipt struct {
	ID         string
	Timestamp  time.Time
	Providence string
}

func Send(ctx context.Context, svc *dynamodb.DynamoDB, message Message, config table.Config) (*dynamodb.PutItemOutput, error) {
	stamp := message.Timestamp.Format(TimestampFormat)
	builder := expression.NewBuilder().WithCondition(expression.Name(config.Attributes.Timestamp).GreaterThan(expression.Value(stamp)))
	if message.Sum != "" {
		builder.WithCondition(expression.Name(config.Attributes.Sum).NotEqual(expression.Value(message.Sum)))
	}
	expr, err := builder.Build()
	if err != nil {
		return nil, err
	}
	input := &dynamodb.PutItemInput{
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		TableName:                 aws.String(config.TableName),
		Item: map[string]*dynamodb.AttributeValue{
			config.Attributes.ID:         {S: aws.String(message.ID)},
			config.Attributes.Timestamp:  {S: aws.String(stamp)},
			config.Attributes.Sum:        {S: aws.String(message.Sum)},
			config.Attributes.Providence: {S: aws.String(message.Providence)},
		},
	}
	for _, destination := range config.Destinations {
		input.Item[destination.StatusAttribute] = &dynamodb.AttributeValue{S: aws.String(message.Status)}
	}
	resp, err := svc.PutItemWithContext(ctx, input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return resp, nil
			}
		}
	}
	return resp, err
}

func Receive(ctx context.Context, svc *dynamodb.DynamoDB, config table.Config, destination table.Destination, status string, count int) ([]Receipt, error) {
	expr, err := expression.NewBuilder().WithProjection(
		expression.NamesList(expression.Name(config.Attributes.Providence), expression.Name(config.Attributes.ID), expression.Name(config.Attributes.Timestamp)),
	).WithKeyCondition(
		expression.KeyEqual(expression.Key(destination.StatusAttribute), expression.Value(status)),
	).Build()
	if err != nil {
		return nil, err
	}
	// TODO: sending a count
	results := []Receipt{}
	err = svc.QueryPagesWithContext(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(config.TableName),
		IndexName:                 aws.String(destination.StatusIndex),
		ProjectionExpression:      expr.Projection(),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		Limit:                     aws.Int64(int64(count)),
	}, func(qo *dynamodb.QueryOutput, b bool) bool {
		for _, out := range qo.Items {
			stamp, _ := time.Parse(TimestampFormat, *out[config.Attributes.Timestamp].S)
			results = append(results, Receipt{
				ID:         *out[config.Attributes.ID].S,
				Timestamp:  stamp,
				Providence: *out[config.Attributes.Providence].S,
			})
		}
		return false
	})
	return results, err
}

func Acknowledge(ctx context.Context, svc *dynamodb.DynamoDB, config table.Config, destination table.Destination, receipts []Receipt, status string) error {
	for _, receipt := range receipts {
		stamp := receipt.Timestamp.Format(TimestampFormat)
		update := expression.Set(expression.Name(destination.StatusAttribute), expression.Value(status))
		condition := expression.Equal(expression.Name(config.Attributes.Timestamp), expression.Value(stamp))
		expr, err := expression.NewBuilder().WithUpdate(update).WithCondition(condition).Build()
		if err != nil {
			return err
		}
		svc.UpdateItem(&dynamodb.UpdateItemInput{
			TableName: aws.String(config.TableName),
			Key: map[string]*dynamodb.AttributeValue{
				config.Attributes.ID: {S: aws.String(receipt.ID)},
			},
			UpdateExpression:          expr.Update(),
			ConditionExpression:       expr.Condition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		})
	}
	return nil
}
