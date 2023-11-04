package pg_connector

import (
	"github.com/jackc/pglogrepl"
)

type flushAwareLSN struct {
	lsn          pglogrepl.LSN
	canBeFlushed bool
}

func newFlushAwareLSNsList() *flushAwareLSNsList {
	return &flushAwareLSNsList{}
}

type flushAwareLSNsList struct {
	list        []flushAwareLSN
	len         int
	lastFlushed pglogrepl.LSN
}

func (l *flushAwareLSNsList) append(lsns ...pglogrepl.LSN) {
	for _, lsn := range lsns {
		if l.len == len(l.list) {
			l.list = append(l.list, flushAwareLSN{lsn: lsn})
			l.len++
			continue
		}

		l.list[l.len].lsn = lsn
		l.list[l.len].canBeFlushed = false
		l.len++
	}
}

func (l *flushAwareLSNsList) canFlushUpTo() pglogrepl.LSN {
	lsn := l.lastFlushed
	for i := 0; i < l.len; i++ {
		if !l.list[i].canBeFlushed {
			break
		}

		lsn = l.list[i].lsn
	}

	if lsn != l.lastFlushed {
		l.lastFlushed = lsn
	}

	return lsn
}

func (l *flushAwareLSNsList) markToBeFlushed(lsn pglogrepl.LSN) {
	for i, marked := 0, 0; i < l.len && marked <= 1; i++ {
		if l.list[i].lsn == lsn {
			l.list[i].canBeFlushed = true
			marked++
		}
	}
}

func (l *flushAwareLSNsList) markToBeFLushedMany(lsns map[pglogrepl.LSN]struct{}) {
	if len(lsns) == 0 {
		return
	}

	for i, marked := 0, 0; i < l.len && marked <= len(lsns); i++ {
		if _, ok := lsns[l.list[i].lsn]; ok {
			l.list[i].canBeFlushed = true
			marked++
		}
	}
}

func (l *flushAwareLSNsList) clearFlushed() {
	var idx int
	for ; idx < l.len; idx++ {
		if !l.list[idx].canBeFlushed {
			break
		}
	}

	for i, j := idx, 0; i < l.len; i, j = i+1, j+1 {
		l.list[j].lsn, l.list[j].canBeFlushed = l.list[i].lsn, l.list[i].canBeFlushed
	}

	l.len = l.len - idx
}
