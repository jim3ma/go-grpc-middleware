package balancer

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
)

const (
	// ConsistentHashRingBalancerName is the name of consistent-hash-ring balancer.
	ConsistentHashRingBalancerName = "consistent-hash-ring"

	// ConsistentHashRingBalancerServiceConfig is a service config that sets the default balancer
	// to the consistent-hash-ring balancer
	ConsistentHashRingBalancerServiceConfig = `{"loadBalancingPolicy":"consistent-hash-ring"}`

	// consistentHashRingCtxKey is the key for the grpc request's context.Context which points to
	// the key to hash for the request. The value it points to must be []byte
	consistentHashRingCtxKey string = "consistent-hash-ring-request-key"
)

var logger = grpclog.Component("consistenthashring")

// NewConsistentHashRingBuilder creates a new balancer.Builder that
// will create a consistent hash ring balancer with the given config.
// Before making a connection, register it with grpc with:
// `balancer.Register(consistent.NewConsistentHashRingBuilder(hasher, factor, spread))`
func NewConsistentHashRingBuilder(hasher HasherFunc, replicationFactor uint16, spread uint8) balancer.Builder {
	return base.NewBalancerBuilder(
		ConsistentHashRingBalancerName,
		&consistentHashRingPickerBuilder{hasher: hasher, replicationFactor: replicationFactor, spread: spread},
		base.Config{HealthCheck: true},
	)
}

type subConnMember struct {
	balancer.SubConn
	key string
}

// Key implements consistent.Member
// This value is what will be hashed for placement on the consistent hash ring.
func (s subConnMember) Key() string {
	return s.key
}

var _ Member = &subConnMember{}

type consistentHashRingPickerBuilder struct {
	hasher            HasherFunc
	replicationFactor uint16
	spread            uint8
}

func (b *consistentHashRingPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	logger.Infof("consistentHashRingPicker: Build called with info: %v", info)
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	hashRing := NewHashRing(b.hasher, b.replicationFactor)
	for sc, scInfo := range info.ReadySCs {
		if err := hashRing.Add(subConnMember{
			SubConn: sc,
			key:     scInfo.Address.Addr + scInfo.Address.ServerName,
		}); err != nil {
			return base.NewErrPicker(err)
		}
	}
	return &consistentHashRingPicker{
		hashRing: hashRing,
		spread:   b.spread,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type consistentHashRingPicker struct {
	sync.Mutex
	hashRing *HashRing
	spread   uint8
	rand     *rand.Rand
}

func (p *consistentHashRingPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	key, ok := info.Ctx.Value(consistentHashRingCtxKey).([]byte)
	if !ok {
		panic(fmt.Sprintf("context key %s not found for consistent hash ring balancer", consistentHashRingCtxKey))
	}
	members, err := p.hashRing.FindN(key, p.spread)
	if err != nil {
		return balancer.PickResult{}, err
	}

	// rand is not safe for concurrent use
	var index int
	if p.spread > 1 {
		p.Lock()
		index = p.rand.Intn(int(p.spread))
		p.Unlock()
	}

	chosen := members[index].(subConnMember)
	return balancer.PickResult{
		SubConn: chosen.SubConn,
	}, nil
}

func ContextWithConsistentHashRingKey(parent context.Context, value []byte) context.Context {
	return context.WithValue(parent, consistentHashRingCtxKey, value)
}
