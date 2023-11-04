package logger

import "time"

type (
	Config struct {
		Mode     string      `yaml:"mode"`
		Level    string      `yaml:"level"`
		Encoding string      `yaml:"encoding"`
		File     *FileConfig `yaml:"file"`
	}

	FileConfig struct {
		Path          string        `yaml:"path"`
		MaxSize       int           `yaml:"max_size"`
		MaxBackups    int           `yaml:"max_backups"`
		MaxAge        int           `yaml:"max_age"`
		BufSize       int           `yaml:"write_buffer_size"`
		FlushInterval time.Duration `yaml:"write_buffer_flush_interval"`
	}
)
