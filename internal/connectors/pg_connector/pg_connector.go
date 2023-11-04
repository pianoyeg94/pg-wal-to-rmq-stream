package pg_connector

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/connectors"
	"github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/util"
)

const (
	dupObjErrCode = "42710"
	maxMsgs       = 1 << 30

	FlushUpToLSNLogField = "flush_up_to_lsn"
	FromLSNLogField      = "from_lsn"
	ToLSNLogField        = "to_lsn"
)

func NewPgConnector(conf Config) *PgConnector {
	return &PgConnector{
		conf:           conf,
		lsns:           newFlushAwareLSNsList(),
		msgIdsToLSNs:   make(map[string]pglogrepl.LSN),
		lsnsToMsgCount: make(map[pglogrepl.LSN]int),
		flushLSNsCh:    make(chan struct{}),
	}
}

type PgConnector struct {
	conf   Config
	logger *zap.Logger
	conn   *pgconn.PgConn

	lsns           *flushAwareLSNsList
	msgIdsToLSNs   map[string]pglogrepl.LSN
	lsnsToMsgCount map[pglogrepl.LSN]int

	processedMsgIdsCh <-chan string
	flushLSNsCh       chan struct{}
	flushedLSNsWg     sync.WaitGroup

	stopStreamingFn    context.CancelFunc
	streamingStoppedWg sync.WaitGroup
	stopFlag           int64
	stopFn             context.CancelFunc
	stoppedWg          sync.WaitGroup
}

func (c *PgConnector) Start(logger *zap.Logger) error {
	c.logger = logger
	return nil
}

func (c *PgConnector) StartStreaming(processedMsgIdsCh <-chan string) (<-chan *connectors.Msg, error) {
	baseCtx := context.Background()
	if err := util.RetryOverTime(
		baseCtx,
		c.startReplication,
		func(ctx context.Context, err error) bool {
			c.logger.Warn("failed starting PostgreSQL replication, retrying", zap.Error(err))
			return true
		},
		1*time.Second, // start
		1*time.Second, // step
		6*time.Second, // stop
		5,             // max
	); err != nil {
		return nil, err
	}

	var ctx context.Context
	msgsCh := make(chan *connectors.Msg)
	c.processedMsgIdsCh = processedMsgIdsCh
	ctx, c.stopStreamingFn = context.WithCancel(baseCtx)
	go c.stream(util.NewStopRequestCtx(ctx, &c.streamingStoppedWg), msgsCh)

	c.stoppedWg.Add(1)
	ctx, c.stopFn = context.WithCancel(baseCtx)
	go c.lsnsFlusher(c.flushLSNsCh, &c.flushedLSNsWg, ctx, &c.stoppedWg)

	return msgsCh, nil
}

func (c *PgConnector) StopStreaming() error {
	c.stopStreaming()
	c.streamingStoppedWg.Wait()
	return nil
}

func (c *PgConnector) Stop() error {
	c.stop()
	c.stoppedWg.Wait()
	close(c.flushLSNsCh)
	return errors.Wrap(
		c.conn.Close(context.Background()),
		"failed closing connection to PostgreSQL server",
	)
}

func (c *PgConnector) stream(stopReq util.StopRequestCtx, msgsCh chan<- *connectors.Msg) {
	standbyTimer := util.NewContextTimer(stopReq.Ctx, c.conf.StandbyTimeout)
	defer func() {
		close(msgsCh)
		stopReq.Wg.Done()
	}()

	var (
		currLSN     pglogrepl.LSN
		danglingMsg *connectors.Msg
	)
	replMsgIterator, _ := NewReplMsgIterator(nil)
streamLoop:
	for {
		select {
		case <-standbyTimer.C():
			if standbyTimer.IsCancelled() {
				return
			}

			c.flushedLSNsWg.Add(1)
			c.flushLSNsCh <- struct{}{}
			c.flushedLSNsWg.Wait()

			standbyTimer.Reset(c.conf.StandbyTimeout)
		default:
		}

		for {
			var msg *connectors.Msg
			msg, danglingMsg = danglingMsg, nil
			if msg == nil {
				var (
					exhausted bool
					err       error
				)
				msg, exhausted, err = replMsgIterator.Next()
				if exhausted {
					if err != nil {
						c.logger.Warn(
							"failed parsing message from PostgreSQL server, dropping message",
							zap.Error(err),
						)
					}
					break
				}
			}

			select {
			case msgsCh <- msg:
				c.msgIdsToLSNs[msg.Id] = currLSN
				c.lsnsToMsgCount[currLSN]++
			case <-standbyTimer.C():
				if standbyTimer.IsCancelled() {
					return
				}

				danglingMsg = msg
				c.lsnsToMsgCount[currLSN] -= maxMsgs

				continue streamLoop
			}
		}

		if c.lsnsToMsgCount[currLSN] < 0 {
			c.lsnsToMsgCount[currLSN] += maxMsgs
		}

		if c.lsnsToMsgCount[currLSN] == 0 {
			c.lsns.markToBeFlushed(currLSN)
		}

		msg, err := c.conn.ReceiveMessage(standbyTimer)
		if err != nil {
			if standbyTimer.IsCancelled() {
				return
			}

			if pgconn.Timeout(err) {
				continue streamLoop
			}

			c.logger.Warn("failed receiving message from PostgreSQL server", zap.Error(err))

			if c.conn.IsClosed() {
				c.logger.Warn("PostgreSQL connnection error occured, recovering", zap.Error(err))
				if err = util.RetryOverTime(
					stopReq.Ctx,
					c.startReplication,
					func(ctx context.Context, err error) bool {
						c.logger.Warn(
							"failed restarting PostgreSQL replication, retrying",
							zap.Error(err),
						)
						return true
					},
					1*time.Second,  // start
					2*time.Second,  // step
					30*time.Second, // stop
					0,              // max
				); err != nil {
					return
				}
			}

			continue streamLoop
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					c.logger.Warn(
						"failed parsing primary keepalive message from PostgreSQL server",
						zap.Error(err),
					)
					continue streamLoop
				}

				if keepalive.ReplyRequested {
					standbyTimer.Reset(0)
				}
			case pglogrepl.XLogDataByteID:
				xlog, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					c.logger.Warn(
						"failed parsing message from PostgreSQL server, dropping message",
						zap.Error(err),
					)
					continue streamLoop
				}

				iterator, err := NewReplMsgIterator(xlog.WALData)
				if err != nil {
					c.logger.Warn(
						"failed to unmarshal message from PostgreSQL server, dropping message",
						zap.Error(err),
					)

					c.lsns.append(xlog.WALStart)
					c.lsns.markToBeFlushed(xlog.WALStart)

					continue streamLoop
				}

				currLSN = iterator.NextLSN()
				c.lsns.append(currLSN)
				replMsgIterator = iterator

				c.logger.Debug(
					"received wal segment from PostgreSQL server",
					zap.String(FromLSNLogField, xlog.WALStart.String()),
					zap.String(ToLSNLogField, currLSN.String()),
				)
			default:
				c.logger.Warn("received unknown message type from PostgreSQL server")
			}
		case *pgproto3.NoticeResponse:
			c.logger.Warn(
				"received error message from PostgreSQL server",
				zap.Error(errors.New(msg.Message)),
			)
		default:
			c.logger.Warn("received unknown message type from PostgreSQL server")
		}
	}
}

func (c *PgConnector) startReplication(ctx context.Context) error {
	// ConnectConfig
	conn, err := pgconn.Connect(ctx, c.conf.ConnectionString+"?replication=database")
	if err != nil {
		return errors.Wrap(err, "failed to connect to PostgreSQL server")
	}

	if _, err = pglogrepl.CreateReplicationSlot(
		ctx,
		conn,
		c.conf.ReplicationSlot,
		c.conf.OutputPlugin,
		pglogrepl.CreateReplicationSlotOptions{},
	); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); !ok || pgErr.SQLState() != dupObjErrCode {
			_ = conn.Close(ctx)
			return errors.Wrapf(
				err,
				"failed to create PostgreSQL logical replication slot %s",
				c.conf.ReplicationSlot,
			)
		}
	}

	if err = pglogrepl.StartReplication(
		ctx,
		conn,
		c.conf.ReplicationSlot,
		0, // startLSN
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				fmt.Sprintf(`"add-tables" '%s'`, c.conf.TableFilter),
				`"include-lsn"`,
			},
		},
	); err != nil {
		_ = conn.Close(ctx)
		return errors.Wrap(err, "failed to start PostgreSQL logical replication")
	}

	c.conn = conn

	return nil
}

func (c *PgConnector) sendStandbyStatusUpdate(ctx context.Context, lsn pglogrepl.LSN) error {
	c.logger.Debug("flushing PostgresQL WAL", zap.String(FlushUpToLSNLogField, lsn.String()))
	if err := pglogrepl.SendStandbyStatusUpdate(
		ctx,
		c.conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn},
	); err != nil {
		if c.conn.IsClosed() {
			if err = util.RetryOverTime(
				ctx,
				c.startReplication,
				func(ctx context.Context, err error) bool {
					c.logger.Error(err.Error())
					return true
				},
				1*time.Second,  // start
				2*time.Second,  // step
				30*time.Second, // stop
				0,              // max
			); err != nil {
				return err
			}
		}
		return err
	}

	return nil
}

func (c *PgConnector) lsnsFlusher(
	flushLSNsCh <-chan struct{},
	flushedLSNsWg *sync.WaitGroup,
	stopCtx context.Context,
	stoppedWg *sync.WaitGroup,
) {
	defer stoppedWg.Done()

	var isStopping bool
	msgIds := util.NewStrsList()
	lsns := make(map[pglogrepl.LSN]struct{})
	for !isStopping {
		var shouldFlush bool
		select {
		case msgId, ok := <-c.processedMsgIdsCh:
			if !ok {
				isStopping = true
				break
			}

			msgIds.Append(msgId)
			select {
			case _, ok := <-flushLSNsCh:
				shouldFlush = true
				if !ok {
					isStopping = true
				}
			default:
			}

			if c.isStopping() {
				isStopping = true
			}
		case _, ok := <-flushLSNsCh:
			shouldFlush = true
			if !ok {
				isStopping = true
			}
		case <-stopCtx.Done():
			isStopping = true
		}

		if !shouldFlush && !isStopping {
			continue
		}

		next := msgIds.Iterator()
		for msgId, exhausted := next(); !exhausted; msgId, exhausted = next() {
			lsn, ok := c.msgIdsToLSNs[msgId]
			if !ok {
				continue
			}
			delete(c.msgIdsToLSNs, msgId)

			if c.lsnsToMsgCount[lsn]--; c.lsnsToMsgCount[lsn] != 0 {
				continue
			}

			delete(c.lsnsToMsgCount, lsn)
			lsns[lsn] = struct{}{}
		}

		c.lsns.markToBeFLushedMany(lsns)
		if err := c.sendStandbyStatusUpdate(stopCtx, c.lsns.canFlushUpTo()); err != nil {
			if stopCtx.Err() == context.Canceled {
				return
			}

			c.logger.Error("failed sending standby status update to PostgreSQL server", zap.Error(err))
		}
		c.lsns.clearFlushed()

		if !isStopping {
			flushedLSNsWg.Done()
		}

		msgIds.Clear()
		for lsn := range lsns {
			delete(lsns, lsn)
		}
	}

}

func (c *PgConnector) isStopping() bool {
	return atomic.LoadInt64(&c.stopFlag) == 1
}

func (c *PgConnector) stopStreaming() {
	if c.stopStreamingFn != nil {
		c.stopStreamingFn()
	}
}

func (c *PgConnector) stop() {
	atomic.StoreInt64(&c.stopFlag, 1)
	if c.stopFn != nil {
		c.stopFn()
	}
}
