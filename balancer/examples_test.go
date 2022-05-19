package balancer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

// Simple example of NewConsistentHashRingBuilder code.
func Example_newConsistentHashRingBuilder() {
	var (
		hashRingReplicationFactor uint16 = 1
		backendsPerKey            uint8  = 1
		wg                        sync.WaitGroup
		once                      sync.Once
	)

	wg.Add(1)

	// initialize a resolver for multiple server addresses
	r := manual.NewBuilderWithScheme("whatever")
	r.InitialState(resolver.State{
		Addresses: []resolver.Address{
			{Addr: "192.168.0.2:50052"},
			{Addr: "192.168.0.3:50052"},
			{Addr: "192.168.0.4:50052"},
		},
	})
	// r.CC only update after built one resolver
	r.BuildCallback = func(resolver.Target, resolver.ClientConn, resolver.BuildOptions) {
		once.Do(func() {
			wg.Done()
		})
	}
	address := fmt.Sprintf("%s:///unused", r.Scheme())

	options := []grpc.DialOption{
		grpc.WithResolvers(r),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"consistent-hash-ring"}`),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// register ConsistentHashRing balancer
	balancer.Register(NewConsistentHashRingBuilder(xxhash.Sum64, hashRingReplicationFactor, backendsPerKey))

	conn, err := grpc.Dial(address, options...)
	if err != nil {
		panic(err)
	}

	go func() {
		// update server addresses
		time.Sleep(5 * time.Second)

		// only call UpdateState after built at least one resolver
		// if did not do this, r.CC may be nil and r.UpdateState will panic
		wg.Done()
		r.UpdateState(resolver.State{
			Addresses: []resolver.Address{
				{Addr: "192.168.0.2:50052"},
				{Addr: "192.168.0.3:50052"},
				{Addr: "192.168.0.5:50052"},
			},
		})
	}()

	client := healthpb.NewHealthClient(conn)
	ctx := ContextWithConsistentHashRingKey(context.Background(), []byte("key-1"))
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		panic(err)
	}
}
