package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/dispatch"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/failure"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/grpcworker"
	kafkaadapter "github.com/aatishgupta25/kafka-consumer-proxy/internal/kafka"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/ports"
)

func main() {
	brokers := splitRequired("KAFKA_BROKERS")
	topic := required("KAFKA_TOPIC")
	groupID := required("KAFKA_GROUP_ID")
	workerTargets := splitRequired("WORKER_TARGETS")
	dlqTopic := required("DLQ_TOPIC")

	consumer := kafkaadapter.NewConsumer(brokers, topic, groupID)
	defer consumer.Close()

	dlq := kafkaadapter.NewDeadLetterWriter(brokers, dlqTopic)
	defer dlq.Close()

	workers := make([]ports.Worker, 0, len(workerTargets))
	clients := make([]*grpcworker.Client, 0, len(workerTargets))
	for _, target := range workerTargets {
		client, err := grpcworker.New(target)
		if err != nil {
			log.Fatalf("connect worker %s: %v", target, err)
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

	maxAttempts := envInt("MAX_ATTEMPTS", 3)
	maxInFlight := envInt("MAX_IN_FLIGHT", 64)
	breakerThreshold := envInt("BREAKER_THRESHOLD", 5)
	breakerCooldown := time.Duration(envInt("BREAKER_COOLDOWN_MS", 2000)) * time.Millisecond

	proxy := dispatch.NewDispatcher(
		consumer,
		pool,
		dlq,
		failure.NewBreaker(breakerThreshold, breakerCooldown),
		maxAttempts,
		maxInFlight,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("starting proxy topic=%s group=%s workers=%d max_in_flight=%d", topic, groupID, len(workers), maxInFlight)
	if err := proxy.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
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
		log.Fatal(fmt.Errorf("%s must be a positive integer", name))
	}
	return parsed
}
