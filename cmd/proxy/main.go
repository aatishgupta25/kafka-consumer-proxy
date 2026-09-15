package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/dispatch"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/failure"
	kafkaadapter "github.com/aatishgupta25/kafka-consumer-proxy/internal/kafka"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/ports"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/transport/grpcworker"
)

func main() {
	brokers := splitRequired("KAFKA_BROKERS")
	topic := required("KAFKA_TOPIC")
	group := required("KAFKA_GROUP")
	dlqTopic := required("DLQ_TOPIC")
	workerTargets := splitRequired("WORKERS")

	consumer := kafkaadapter.NewConsumer(brokers, topic, group)
	defer consumer.Close()
	dlq := kafkaadapter.NewDLQWriter(brokers, dlqTopic)
	defer dlq.Close()

	workers := make([]ports.Worker, 0, len(workerTargets))
	clients := make([]*grpcworker.Client, 0, len(workerTargets))
	for _, target := range workerTargets {
		client, err := grpcworker.Dial(target)
		if err != nil {
			log.Fatalf("dial worker %s: %v", target, err)
		}
		clients = append(clients, client)
		workers = append(workers, client)
	}
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()

	pool, err := dispatch.NewPool(workers)
	if err != nil {
		log.Fatal(err)
	}

	maxInFlight := envInt("MAX_IN_FLIGHT", 64)
	maxAttempts := envInt("MAX_ATTEMPTS", 3)
	breaker := failure.NewBreaker(envInt("BREAKER_FAILURES", 5), time.Duration(envInt("BREAKER_COOLDOWN_MS", 1000))*time.Millisecond)
	proxy := dispatch.NewDispatcher(consumer, pool, dlq, breaker, maxAttempts, maxInFlight)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := proxy.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("proxy stopped: %v", err)
	}
}

func required(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}

func splitRequired(name string) []string {
	parts := strings.Split(required(name), ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			out = append(out, value)
		}
	}
	if len(out) == 0 {
		log.Fatalf("%s must contain at least one value", name)
	}
	return out
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		log.Fatalf("%s must be a positive integer", name)
	}
	return parsed
}
