package pg_connector

import (
	json "encoding/json"

	"github.com/jackc/pglogrepl"
	"github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/connectors"
	"github.com/pkg/errors"
)

const (
	skipChangeKind         = "delete"
	walDataUnmarshalErrMsg = "failed to unmarshal WAL data"
)

func NewReplMsgIterator(walData []byte) (*ReplMsgIterator, error) {
	var iterator ReplMsgIterator
	if len(walData) == 0 {
		iterator.isExhausted = true
		return &iterator, nil
	}

	if err := iterator.UnmarshalJSON(walData); err != nil {
		iterator.isExhausted = true
		return &iterator, err
	}

	var err error
	if iterator.nextLSN, err = pglogrepl.ParseLSN(iterator.NextLSNStr); err != nil {
		return nil, err
	}

	return &iterator, nil
}

//easyjson:json
type ReplMsgIterator struct {
	currIdx     int
	isExhausted bool
	nextLSN     pglogrepl.LSN
	NextLSNStr  string `json:"nextlsn"`
	Changes     []struct {
		Kind       string        `json:"kind"`
		Columnvals []interface{} `json:"columnvalues"`
	} `json:"change"`
}

func (i *ReplMsgIterator) Next() (_ *connectors.Msg, exhausted bool, err error) {
	if i.isExhausted {
		return nil, true, nil
	}

	if i.currIdx >= len(i.Changes) {
		i.isExhausted = true
		return nil, true, nil
	}

	for i.Changes[i.currIdx].Kind == skipChangeKind {
		i.currIdx++
		if i.currIdx >= len(i.Changes) {
			i.isExhausted = true
			return nil, true, nil
		}
	}

	msg, err := i.columnvalsToSrcMsg(i.Changes[i.currIdx].Columnvals)
	if err != nil {
		i.isExhausted = true
		return nil, true, err
	}

	i.currIdx++

	return msg, false, nil
}

func (i *ReplMsgIterator) NextLSN() pglogrepl.LSN {
	return i.nextLSN
}

func (i *ReplMsgIterator) columnvalsToSrcMsg(columnvals []interface{}) (*connectors.Msg, error) {
	id, ok := columnvals[0].(string)
	if !ok {
		return nil, errors.Wrap(errors.New("id should be a string"), walDataUnmarshalErrMsg)
	}

	exchange, ok := columnvals[1].(string)
	if !ok {
		return nil, errors.Wrap(errors.New("exchange should be a string"), walDataUnmarshalErrMsg)
	}

	exchangeKind, ok := columnvals[2].(string)
	if !ok {
		return nil, errors.Wrap(errors.New("exchange_kind should be a string"), walDataUnmarshalErrMsg)
	}

	routingKey, ok := columnvals[3].(string)
	if !ok {
		return nil, errors.Wrap(errors.New("routing_key should be a string"), walDataUnmarshalErrMsg)
	}

	rawHeaders, ok := columnvals[4].(string)
	if !ok {
		return nil, errors.Wrap(errors.New("headers should be in json format"), walDataUnmarshalErrMsg)
	}
	headers := make(map[string]interface{})
	if err := json.Unmarshal([]byte(rawHeaders), &headers); err != nil {
		return nil, errors.Wrap(err, walDataUnmarshalErrMsg)
	}

	contentType, ok := columnvals[5].(string)
	if !ok {
		return nil, errors.Wrap(errors.New("content_type should be a string"), walDataUnmarshalErrMsg)
	}

	contentEncoding, ok := columnvals[6].(string)
	if !ok {
		return nil, errors.Wrap(errors.New("content_encoding should be a string"), walDataUnmarshalErrMsg)
	}

	deliveryMode, ok := columnvals[7].(string)
	if !ok {
		return nil, errors.Wrap(errors.New("delivery_mode should be a string"), walDataUnmarshalErrMsg)
	}

	body, ok := columnvals[8].(string)
	if !ok {
		return nil, errors.Wrap(errors.New("body should be of type json"), walDataUnmarshalErrMsg)
	}

	return connectors.NewMsg(
		id,
		exchange,
		exchangeKind,
		routingKey,
		headers,
		contentType,
		contentEncoding,
		deliveryMode,
		[]byte(body),
	), nil
}
