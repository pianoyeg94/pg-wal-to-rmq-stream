package logger

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	DevModeStr = "development"
	DevMode    = iota + 1
	ProdMode
)

const (
	RmqMsgIdField      = "message_id"
	RmqRetryDelayField = "retry_delay"
)

func NewLogger(
	modeStr string,
	level string,
	encoding string,
	fcfg *FileConfig,
) (*zap.Logger, func() error, error) {
	mode := ProdMode
	if strings.ToLower(modeStr) == DevModeStr {
		mode = DevMode
	}

	enc := buildEncoder(encoding, mode)
	sink, close := buildSink(fcfg)
	opts := buildOptions(mode, sink)
	lvl, err := zapcore.ParseLevel(level)
	if err != nil {
		return nil, nil, err
	}

	enab := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		return l >= lvl
	})

	core := zapcore.NewCore(enc, sink, enab)

	return zap.New(core, opts...), close, nil
}

func buildEncoder(enc string, mode int) zapcore.Encoder {
	cfg := zap.NewProductionEncoderConfig()
	if mode == DevMode {
		cfg = zap.NewDevelopmentEncoderConfig()
	}

	e := zapcore.NewConsoleEncoder(cfg)
	if enc == "json" {
		e = zapcore.NewJSONEncoder(cfg)
	}

	return e
}

func buildOptions(mode int, sink zapcore.WriteSyncer) []zap.Option {
	opts := []zap.Option{
		zap.AddStacktrace(zap.ErrorLevel),
		zap.ErrorOutput(sink),
	}

	if mode == DevMode {
		opts = append(opts, zap.Development(), zap.AddCaller())
	}

	return opts
}

func buildSink(fcfg *FileConfig) (zapcore.WriteSyncer, func() error) {
	sink := zapcore.Lock(os.Stderr)
	close := func() error { return nil }
	if fcfg != nil {
		fsink := newFileSink(
			fcfg.Path,
			fcfg.MaxSize,
			fcfg.MaxBackups,
			fcfg.MaxAge,
			fcfg.BufSize,
			fcfg.FlushInterval,
		)

		sink = zap.CombineWriteSyncers(sink, fsink)
		close = fsink.Close
	}

	return sink, close
}
