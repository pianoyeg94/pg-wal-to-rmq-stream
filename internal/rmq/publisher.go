package rmq

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/util"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

const contentType = "application/json; charset=utf-8"

func newPublisher(
	cfg Config,
	logger *zap.Logger,
	conn *amqp.Connection,
	msgs <-chan *Msg,
	danglingMsg chan *Msg,
) *publisher {
	return &publisher{
		cfg:         cfg,
		logger:      logger,
		conn:        conn,
		msgs:        msgs,
		danglingMsg: danglingMsg,
		published:   make(chan string),
		flowControl: conn.NotifyBlocked(make(chan amqp.Blocking, 1)),
	}
}

type publisher struct {
	cfg         Config
	logger      *zap.Logger
	conn        *amqp.Connection
	chann       *amqp.Channel
	msgs        <-chan *Msg
	danglingMsg chan *Msg
	acks        <-chan uint64
	nacks       <-chan uint64
	published   chan string
	flowControl <-chan amqp.Blocking

	stop     chan struct{}
	stopFlag uint64
	runDone  sync.WaitGroup
	done     sync.WaitGroup
}

func (p *publisher) start() (<-chan string, error) {
	var err error
	if p.chann, err = p.createChann(); err != nil {
		return nil, err
	}

	p.acks, p.nacks = p.chann.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))

	p.runDone.Add(1)
	p.done.Add(1)
	go p.run()

	p.done.Add(1)
	go p.watchChannErrs()

	return p.published, nil
}

func (p *publisher) close() error {
	p.setStop()
	close(p.stop)
	p.done.Wait()

	return p.chann.Close()
}

func (p *publisher) run() {
	defer func() {
		p.runDone.Done()
		p.done.Done()
	}()

	msgs := p.msgs
	select {
	case msg, ok := <-p.danglingMsg:
		if ok {
			p.danglingMsg <- msg
			msgs = p.danglingMsg
		}
	default:
	}

	channErrs := p.chann.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case msg, ok := <-msgs:
			if msgs != p.msgs {
				msgs = p.msgs
			}

			if !ok || p.shouldStop() {
				p.danglingMsg <- msg
				return
			}

			if err := util.RetryOverTime(context.Background(),
				func(ctx context.Context) error {
					if p.maybePauseOnFlowControl() && p.shouldStop() {
						p.danglingMsg <- msg
						return nil
					}

					if err := p.publish(msg); err != nil {
						return err
					}

					return p.waitConfirm()
				},
				func(ctx context.Context, err error) bool {
					select {
					case <-channErrs:
						return false
					default:
						return err == ErrNack && !p.shouldStop()
					}
				},
				p.cfg.Republish.DelayStart,
				p.cfg.Republish.DelayStep,
				p.cfg.Republish.DelayStop,
				p.cfg.Republish.MaxAttempts,
			); err != nil && err != ErrInvalidHeaders {
				p.danglingMsg <- msg
				return
			}

			p.published <- msg.Id
		case <-channErrs:
			return
		case <-p.stop:
			return
		}
	}
}

func (p *publisher) publish(msg *Msg) error {
	publishing := amqp.Publishing{
		MessageId:    msg.Id,
		ContentType:  contentType,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now().UTC(),
		Body:         msg.Body,
	}
	if err := p.chann.Publish(p.cfg.Exchange, p.cfg.RoutingKey, false, false, publishing); err != nil {
		if errv := publishing.Headers.Validate(); errv != nil {
			return ErrInvalidHeaders
		}
		return err
	}

	return nil
}

func (p *publisher) createChann() (*amqp.Channel, error) {
	chann, err := p.conn.Channel()
	if err != nil {
		return nil, err
	}

	return chann, chann.Confirm(false)
}

func (p *publisher) waitConfirm() error {
	var ok bool
	select {
	case _, ok = <-p.acks:
	case _, ok = <-p.nacks:
		if ok {
			return ErrNack
		}
	}

	if !ok {
		return amqp.ErrClosed
	}

	return nil
}

func (p *publisher) watchChannErrs() {
	defer p.done.Done()

	connErrs := p.conn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case _, ok := <-p.chann.NotifyClose(make(chan *amqp.Error, 1)):
			if !ok {
				return
			}

			select {
			case <-connErrs:
				return
			default:
			}
		case <-connErrs:
			return
		}

		p.runDone.Wait()

		if err := util.RetryOverTime(context.Background(),
			func(ctx context.Context) error {
				var err error
				if p.chann, err = p.createChann(); err != nil {
					return err
				}

				p.acks, p.nacks = p.chann.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))

				return nil
			},
			func(ctx context.Context, err error) bool {
				select {
				case <-connErrs:
					return false
				default:
					return !p.shouldStop()
				}
			},
			p.cfg.Reconnect.DelayStart,
			p.cfg.Reconnect.DelayStep,
			p.cfg.Reconnect.DelayStop,
			-1,
		); err != nil {
			return
		}

		p.runDone.Add(1)
		p.done.Add(1)
		go p.run()
	}
}

func (p *publisher) maybePauseOnFlowControl() bool {
	select {
	case block, ok := <-p.flowControl:
		for ok && block.Active && !p.shouldStop() {
			select {
			case block, ok = <-p.flowControl:
			case <-p.stop:
			}
		}

		return true
	default:
	}

	return false
}

func (p *publisher) setStop() {
	atomic.AddUint64(&p.stopFlag, 1)
}

func (p *publisher) shouldStop() bool {
	return atomic.LoadUint64(&p.stopFlag) >= 1
}
