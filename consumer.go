package main

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/asstart/notion-inbox-publisher/message"
	"github.com/dstotijn/go-notion"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/proto"
)

type Consumer struct {
	ready        chan bool
	notionClient *notion.Client
	dbID         string
	logger       logr.Logger
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():

			if err := c.handleMsg(msg); err != nil {
				c.logger.Info("error while creating notion page", "error", err)
			}
			session.MarkMessage(msg, "")
			session.Commit()
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *Consumer) handleMsg(msg *sarama.ConsumerMessage) error {
	decoded, err := c.decodeMessage(msg.Value)
	if err != nil {
		return err
	}

	req := c.createPageRq(decoded)

	_, err = c.notionClient.CreatePage(context.Background(), req)
	if err != nil {
		return err
	}

	return nil
}

func (c *Consumer) decodeMessage(bytes []byte) (*message.Message, error) {
	var msg message.Message
	if err := proto.Unmarshal(bytes, &msg); err != nil {
		return nil, fmt.Errorf("cant't unmarhsall protobuff message: %w", err)
	}
	return &msg, nil
}

func (c *Consumer) createPageRq(msg *message.Message) notion.CreatePageParams {
	return notion.CreatePageParams{
		ParentType: notion.ParentTypeDatabase,
		ParentID:   c.dbID,
		DatabasePageProperties: &notion.DatabasePageProperties{
			"Message": notion.DatabasePageProperty{
				RichText: []notion.RichText{
					{
						Text: &notion.Text{
							Content: string(msg.Content)},
					},
				},
			},
		},
	}
}
