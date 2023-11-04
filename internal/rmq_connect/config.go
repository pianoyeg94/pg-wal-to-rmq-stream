package rmq_connect

import (
	"github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/connectors/pg_connector"
	"github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/logger"
	"github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/rmq"
)

type Config struct {
	Rmq       rmq.Config          `yaml:"rmq"`
	Connector pg_connector.Config `yaml:"connector"`
	Logging   logger.Config       `yaml:"logging"`
}
