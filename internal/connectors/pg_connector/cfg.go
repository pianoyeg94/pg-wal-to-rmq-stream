package pg_connector

import "time"

type Config struct {
	ConnectionString string        `yaml:"connection_string"`
	ReplicationSlot  string        `yaml:"replication_slot"`
	OutputPlugin     string        `yaml:"output_plugin"`
	TableFilter      string        `yaml:"table_filter"`
	StandbyTimeout   time.Duration `yaml:"standby_timeout"`
}
