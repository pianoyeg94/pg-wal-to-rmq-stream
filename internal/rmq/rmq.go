package rmq

import (
	"context"
	"hash"
	"hash/fnv"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
	"go.uber.org/zap"

	"github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/util"
)

var (
	ErrInvalidHeaders = &amqp.Error{Code: amqp.CommandInvalid, Reason: "invalid headers"}
	ErrNack           = &amqp.Error{Reason: "publishing nacked", Recover: true}
)

type publishedChan chan string

func (c publishedChan) fanIn(ch <-chan string) {
	go func() {
		for msgId := range ch {
			c <- msgId
		}
	}()
}

type Msg struct {
	Id   string
	Key  string
	Body []byte
}

func NewMsg(id string, key string, body []byte) *Msg {
	return &Msg{
		Id:   id,
		Key:  key,
		Body: body,
	}
}

func NewRMQ(cfg Config, logger *zap.Logger, msgs <-chan *Msg) *RMQ {
	msgPartitions := make([]chan *Msg, cfg.Concurrency)
	for i := range msgPartitions {
		msgPartitions[i] = make(chan *Msg, 1024)
	}

	danglingMsgs := make([]chan *Msg, cfg.Concurrency)
	for i := range danglingMsgs {
		danglingMsgs[i] = make(chan *Msg, 1)
	}

	return &RMQ{
		cfg:           cfg,
		logger:        logger,
		hasher:        fnv.New32a(),
		publishers:    make([]*publisher, cfg.Concurrency),
		msgs:          msgs,
		msgPartitions: msgPartitions,
		danglingMsgs:  danglingMsgs,
		published:     make(publishedChan),
	}
}

type RMQ struct {
	cfg           Config
	logger        *zap.Logger
	conn          *amqp.Connection
	hasher        hash.Hash32
	publishers    []*publisher
	msgs          <-chan *Msg
	msgPartitions []chan *Msg
	danglingMsgs  []chan *Msg
	published     publishedChan

	stop     chan struct{}
	stopFlag uint64
	stopDone sync.WaitGroup
}

func (r *RMQ) StartPublishing() (<-chan string, error) {
	var err error
	if r.conn, err = r.connect(); err != nil {
		return nil, err
	}

	if err = r.startPublishers(); err != nil {
		return nil, err
	}

	go r.publish()
	go r.watchConnErrs()

	return r.published, nil
}

func (r *RMQ) StopPublishing() error {
	return nil
}

func (r *RMQ) connect() (*amqp.Connection, error) {
	var dialer func(network string, addr string) (net.Conn, error)
	if r.cfg.ConnectionTimeout >= 0 {
		dialer = r.dial
	}

	conn, err := amqp.DialConfig(r.cfg.ConnectionString, amqp.Config{
		Heartbeat: r.cfg.Heartbeat,
		Dial:      dialer,
	})
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (r *RMQ) dial(network string, addr string) (net.Conn, error) {
	conn, err := net.DialTimeout(network, addr, r.cfg.ConnectionTimeout)
	if err != nil {
		return nil, err
	}

	// Heartbeating hasn't started yet, don't stall forever on a dead server.
	// A deadline is set for TLS and AMQP handshaking. After AMQP is established,
	// the deadline is cleared.
	if err = conn.SetDeadline(time.Now().Add(r.cfg.ConnectionTimeout)); err != nil {
		return nil, err
	}

	return conn, nil
}

func (r *RMQ) startPublishers() error {
	var err error
	for i := 0; i < r.cfg.Concurrency; i++ {
		if r.publishers[i], err = r.startPublisher(i); err != nil {
			r.closePublishers()
			return err
		}
	}

	return nil
}

func (r *RMQ) startPublisher(partition int) (*publisher, error) {
	publisher := newPublisher(r.cfg, r.logger, r.conn, r.msgPartitions[partition], r.danglingMsgs[partition])
	published, err := publisher.start()
	if err != nil {
		return nil, err
	}

	r.published.fanIn(published)

	return publisher, nil
}

func (r *RMQ) closePublishers() {
	var wg sync.WaitGroup
	for _, p := range r.publishers {
		wg.Add(1)
		go func(p *publisher) {
			defer wg.Done()
			_ = p.close()
		}(p)
	}
	wg.Wait()
}

func (r *RMQ) publish() {
	for {
		select {
		case msg, ok := <-r.msgs:
			if !ok || r.shouldStop() {
				return
			}

			select {
			case r.getMsgPartition(msg) <- msg:
			case <-r.stop:
				return
			}
		case <-r.stop:
			return
		}
	}
}

func (r *RMQ) getMsgPartition(msg *Msg) chan *Msg {
	r.hasher.Reset()
	if _, err := r.hasher.Write([]byte(msg.Key)); err != nil {
		panic(err)
	}

	partition := int32(r.hasher.Sum32()) % int32(len(r.msgPartitions))
	if partition < 0 {
		partition = -partition
	}

	return r.msgPartitions[int(partition)]
}

func (r *RMQ) watchConnErrs() {
	for {
		select {
		case _, ok := <-r.conn.NotifyClose(make(chan *amqp.Error, 1)):
			if !ok || r.shouldStop() {
				return
			}

			r.closePublishers()

			if err := util.RetryOverTime(context.Background(),
				func(ctx context.Context) error {
					var err error
					if r.conn, err = r.connect(); err != nil {
						return err
					}

					return r.startPublishers()
				},
				func(ctx context.Context, err error) bool {
					return ctx.Err() != context.Canceled
				},
				r.cfg.Reconnect.DelayStart,
				r.cfg.Reconnect.DelayStep,
				r.cfg.Reconnect.DelayStop,
				-1,
			); err != nil {
				return
			}
		case <-r.stop:
			return
		}
	}
}

func (r *RMQ) setStop() {
	atomic.AddUint64(&r.stopFlag, 1)
}

func (r *RMQ) shouldStop() bool {
	return atomic.LoadUint64(&r.stopFlag) >= 1
}
