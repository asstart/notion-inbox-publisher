package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/go-logr/logr"
	"github.com/go-logr/zerologr"
	"github.com/jessevdk/go-flags"
	"github.com/rs/zerolog"

	"github.com/dstotijn/go-notion"
)

type opts struct {
	Brokers      string `short:"b" long:"brokers" env:"KAFKA_BROKERS" description:"Kafka brokers list" required:"true"`
	DefaultTopic string `short:"s" long:"default-topic" env:"DEFAULT_TOPIC" description:"Default topic for messages" required:"false" default:"messages"`
	NotionToken  string `short:"t" long:"notion-token" env:"NOTION_TOKEN" description:"Notion token" required:"true"`
	NotionDB     string `short:"d" long:"notion-db" env:"NOTION_DB" description:"Notion database" required:"true"`
}

func (o *opts) String() string {
	return fmt.Sprintf(`
	Brokers: %s
	Default topic: %s
	`,
		o.Brokers, o.DefaultTopic)
}

func main() {

	var o opts
	if _, err := flags.Parse(&o); err != nil {
		log.Printf("error while parsing flags: %s", err)
		os.Exit(1)
	}

	log.Printf("starting notion publisher with options: %s", o.String())

	config := consumerConf(o)

	notionClient := setupNotionClient(o)

	logger := setupLogger()

	consumer := Consumer{
		ready:        make(chan bool),
		notionClient: notionClient,
		dbID:         o.NotionDB,
		logger:       logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(o.Brokers, ","), "1", config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err = client.Consume(ctx, []string{o.DefaultTopic}, &consumer); err != nil {
				logger.Error(err, "error from consumer")
				os.Exit(1)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	logger.Info("consumer up and running")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			logger.Info("terminating: context canceled")
			keepRunning = false
		case <-sigterm:
			logger.Info("terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(client, &consumptionIsPaused, logger)
		}
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		logger.Error(err, "error closing client")
	}
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool, logger logr.Logger) {
	if *isPaused {
		client.ResumeAll()
		logger.Info("resuming consumption")
	} else {
		client.PauseAll()
		logger.Info("pausing consumption")
	}

	*isPaused = !*isPaused
}

func consumerConf(_ opts) *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Offsets.AutoCommit.Enable = false
	return config
}

func setupLogger() logr.Logger {
	return stdoutLogger()
}

func stdoutLogger() logr.Logger {
	zl := zerolog.New(os.Stdout)
	zl = zl.With().Caller().Timestamp().Logger()
	return zerologr.New(&zl)
}

func setupNotionClient(o opts) *notion.Client {
	return notion.NewClient(o.NotionToken)
}


