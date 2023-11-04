package rmq_connect

import (
	"os"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/connectors"
	"github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/connectors/pg_connector"
	"github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/logger"
	"github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/rmq"
)

func NewRmqConnect(cfgPath string) *RmqConnect {
	return &RmqConnect{
		cfgPath: cfgPath,
	}
}

type RmqConnect struct {
	cfgPath   string
	cfg       Config
	rmq       *rmq.RMQ
	connector connectors.Connector
	logger    *zap.Logger
	closeLog  func() error
	stopCh    chan struct{}
}

func (c *RmqConnect) Start() error {
	if err := c.loadConfig(); err != nil {
		return err
	}

	var err error
	if c.logger, c.closeLog, err = logger.NewLogger(
		c.cfg.Logging.Mode,
		c.cfg.Logging.Level,
		c.cfg.Logging.Encoding,
		c.cfg.Logging.File,
	); err != nil {
		return err
	}

	c.logger.Info("bringing up RabbitMQ Connect server")

	c.stopCh = make(chan struct{})
	rmqMsgsCh := make(chan *rmq.Msg)
	connectorProcessedCh := make(chan string)
	c.rmq = rmq.NewRMQ(c.cfg.Rmq, c.logger, rmqMsgsCh)
	c.connector = pg_connector.NewPgConnector(c.cfg.Connector)

	rmqProcessedCh, err := c.rmq.StartPublishing()
	if err != nil {
		return err
	}
	c.logger.Info("ready to publish messages to RabbitMQ")

	if err = c.connector.Start(c.logger); err != nil {
		if err1 := c.rmq.StopPublishing(); err1 != nil {
			return errors.Wrap(err1, err.Error())
		}
		return err
	}

	connectorMsgsCh, err := c.connector.StartStreaming(connectorProcessedCh)
	if err != nil {
		if err1 := c.rmq.StopPublishing(); err1 != nil {
			return errors.Wrap(err1, err.Error())
		}
		return err
	}
	c.logger.Info("ready to stream messages from connector source")

	go c.msgsProxy(rmqMsgsCh, connectorMsgsCh)
	go c.processedProxy(connectorProcessedCh, rmqProcessedCh)

	c.logger.Info("RabbitMQ Connect server running")

	return nil
}

func (c *RmqConnect) Stop() {
	c.logger.Info("received termination signal, shutting down")

	c.logger.Info("stopping messages stream from connector source")
	if err := c.connector.StopStreaming(); err != nil {
		c.logger.Error("failed to cleanly stop connector message streaming", zap.Error(err))
	}

	c.logger.Info("stopping message publications to RabbitMQ")
	if err := c.rmq.StopPublishing(); err != nil {
		c.logger.Error("failed to cleanly stop publishing to RabbitMQ", zap.Error(err))
	}

	close(c.stopCh)

	c.logger.Info("stopping connector source")
	if err := c.connector.Stop(); err != nil {
		c.logger.Error("failed to cleanly stop connector", zap.Error(err))
	}

	c.logger.Info("shutdown complete")
}

func (c *RmqConnect) loadConfig() error {
	file, err := os.Open(c.cfgPath)
	if err != nil {
		return errors.Wrap(err, "can't open config file")
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	if err = decoder.Decode(&c.cfg); err != nil {
		return errors.Wrap(err, "can't decode config file")
	}

	return nil
}

func (c *RmqConnect) msgsProxy(
	rmqMsgsCh chan<- *rmq.Msg,
	connectorMsgsCh <-chan *connectors.Msg,
) {
	defer close(rmqMsgsCh)
	for {
		select {
		case connectorMsg, ok := <-connectorMsgsCh:
			if !ok {
				return
			}

			select {
			case rmqMsgsCh <- rmq.NewMsg(connectorMsg.Id, connectorMsg.RoutingKey, connectorMsg.Body):
			case <-c.stopCh:
				return
			}
		case <-c.stopCh:
			return
		}
	}
}

func (c *RmqConnect) processedProxy(
	connectorProcessedCh chan<- string,
	rmqProcessedCh <-chan string,
) {
	defer close(connectorProcessedCh)
	for {
		select {
		case msgId, ok := <-rmqProcessedCh:
			if !ok {
				return
			}

			select {
			case connectorProcessedCh <- msgId:
			case <-c.stopCh:
				return
			}
		case <-c.stopCh:
			return
		}
	}
}
