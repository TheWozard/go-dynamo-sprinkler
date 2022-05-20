package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/TheWozard/go-dynamo-sprinkler/pkg/message"
	"github.com/TheWozard/go-dynamo-sprinkler/pkg/table"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/urfave/cli/v2"
)

const (
	TableFlag    = "table"
	EndpointFlag = "endpoint"
	RegionFlag   = "region"

	DelayFlag = "delay"
	CountFlag = "count"
	SeedFlag  = "seed"
)

var (
	config = table.DefaultConfig
)

func main() {
	input := NewInput()
	app := &cli.App{
		Name:        "go-dynamo-sprinkler",
		Description: "basic tooling for setting up dynamo infrastructure",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    TableFlag,
				Aliases: []string{"t"},
				Value:   config.TableName,
				Usage:   "Name of the table to work against",
			},
			&cli.StringFlag{
				Name:    EndpointFlag,
				Aliases: []string{"e"},
				Value:   "http://localhost:8000",
				Usage:   "Endpoint to hit for accessing the table",
			},
			&cli.StringFlag{
				Name:    RegionFlag,
				Aliases: []string{"r"},
				Value:   "us-east-1",
				Usage:   "Endpoint to hit for accessing the table",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "init",
				Aliases: []string{"i"},
				Usage:   "Creates a local table for working against. Will ask to delete table if one already exists.",
				Action: func(c *cli.Context) error {
					svc := BuildLocalClient(c)
					config.TableName = c.String(TableFlag)
					if table.Exits(c.Context, svc, config) {
						if input.Confirm("Would you like to delete the previous table?") {
							_, err := table.Delete(c.Context, svc, config)
							if err != nil {
								return err
							}
						} else {
							return nil
						}
					}
					_, err := table.Create(c.Context, svc, config)
					return err
				},
			},
			{
				Name:    "stream",
				Aliases: []string{"s"},
				Usage:   "streams random data into the store",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:    DelayFlag,
						Aliases: []string{"d"},
						Value:   10,
						Usage:   "ms to wait between messages",
					},
					&cli.IntFlag{
						Name:    CountFlag,
						Aliases: []string{"c"},
						Value:   100,
						Usage:   "max messages to send. -1 sends indefinitely",
					},
					&cli.Int64Flag{
						Name:    SeedFlag,
						Aliases: []string{"s"},
						Value:   time.Now().Unix(),
						Usage:   "seed for go-langs random number generator",
					},
				},
				Action: func(c *cli.Context) error {
					svc := BuildLocalClient(c)
					config.TableName = c.String(TableFlag)
					delay := c.Int(DelayFlag)
					current := 0
					count := c.Int(CountFlag)
					rand.Seed(c.Int64(SeedFlag))
					for current != count {
						m := message.Message{
							ID:         fmt.Sprintf("%d", current),
							Timestamp:  time.Now().In(time.UTC),
							Sum:        fmt.Sprintf("%d", rand.Intn(4000)),
							Providence: "https://localhost:8000/",
							Status:     message.StatusReady,
						}
						_, err := message.Send(c.Context, svc, m, config)
						if err != nil {
							return err
						}
						current++
						fmt.Printf("%d: %+v\n", current, m)
						time.Sleep(time.Duration(delay) * time.Millisecond)
					}
					return nil
				},
			},
			{
				Name:    "receive",
				Aliases: []string{"r"},
				Usage:   "receives records from the sprinkler",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:    CountFlag,
						Aliases: []string{"c"},
						Value:   100,
						Usage:   "number of messages to receive",
					},
				},
				Action: func(c *cli.Context) error {
					svc := BuildLocalClient(c)
					config.TableName = c.String(TableFlag)
					receipts, err := message.Receive(c.Context, svc, config, table.PrimaryDestination, message.StatusReady, c.Int(CountFlag))
					if err != nil {
						return err
					}
					for _, m := range receipts {
						fmt.Printf("%+v\n", m)
					}
					if len(receipts) > 0 && input.Confirm("Would you like to register the messages as received") {
						err := message.Acknowledge(c.Context, svc, config, table.PrimaryDestination, receipts, message.StatusDelivered)
						if err != nil {
							return err
						}
						fmt.Println("Messages acknowledged")
					}
					return nil
				},
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

// BuildLocalClient build a *dynamodb.DynamoDB for interacting with a local dynamodb
func BuildLocalClient(context *cli.Context) *dynamodb.DynamoDB {
	session, err := session.NewSession(aws.NewConfig().WithRegion(context.String(RegionFlag)).WithEndpoint(context.String(EndpointFlag)))
	if err != nil {
		log.Fatal(err)
	}
	return dynamodb.New(session)
}

// NewInput creates a new Input for interacting with the user
func NewInput() Input {
	return Input{
		reader: bufio.NewReader(os.Stdin),
	}
}

// Input helper struct for handling common interactions with the user
type Input struct {
	reader *bufio.Reader
}

// Confirm writes the message to the user and ways for yes/no response
func (i Input) Confirm(message string) bool {
	fmt.Printf("%s (y/N) ", message)
	text, err := i.reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	lower := strings.ToLower(strings.TrimSpace(text))
	return lower == "y" || lower == "yes"
}

// Pauses until the user pressed enter. Will exit when anything else is returned
func (i Input) Continue() bool {
	fmt.Printf("Press 'Enter' to continue")
	// TODO: This could be improved to instantly exist when the user presses any key other then enter
	text, err := i.reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	trimmed := strings.TrimSpace(text)
	return trimmed == ""
}
