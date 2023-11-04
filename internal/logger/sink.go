package logger

import (
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

func newFileSink(
	path string,
	maxSize int,
	maxBackups int,
	maxAge int,
	bufSize int,
	flushInterval time.Duration,
) *fileSink {
	var sink fileSink
	sink.fws = fileSyncWriter{lumberjack.Logger{
		Filename:   path,
		MaxSize:    maxSize,
		MaxBackups: maxBackups,
		MaxAge:     maxAge,
	}}

	sink.BufferedWriteSyncer = zapcore.BufferedWriteSyncer{
		WS:            &sink.fws,
		Size:          bufSize,
		FlushInterval: flushInterval,
	}

	return &sink
}

var _ zapcore.WriteSyncer = (*fileSink)(nil)

type fileSink struct {
	zapcore.BufferedWriteSyncer
	fws fileSyncWriter
}

func (s *fileSink) Close() error {
	var errs []error
	if err := s.BufferedWriteSyncer.Stop(); err != nil {
		errs = append(errs, err)
	}

	if err := s.fws.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return multierr.Combine(errs...)
	}

	return nil
}

type fileSyncWriter struct {
	lumberjack.Logger
}

func (sw *fileSyncWriter) Sync() error { return nil }
