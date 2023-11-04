package util

func NewStrsList() *StrsList {
	return &StrsList{}
}

type StrsList struct {
	list []string
	len  int
}

func (l *StrsList) Append(strs ...string) {
	for i, str := range strs {
		if l.len == len(l.list) {
			l.list = append(l.list, strs[i:]...)
			l.len += len(strs[i:])
			return
		}

		l.list[l.len] = str
		l.len++
	}
}

func (l *StrsList) Iterator() (next func() (str string, exhausted bool)) {
	var idx int
	return func() (str string, exhausted bool) {
		if idx >= l.len {
			return "", true
		}

		idx++
		return l.list[idx-1], false
	}
}

func (l *StrsList) Len() int {
	return l.len
}

func (l *StrsList) Clear() {
	l.len = 0
}
