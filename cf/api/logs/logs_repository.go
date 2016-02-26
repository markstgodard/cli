package logs

import "time"

type Loggable interface {
	ToLog(loc *time.Location) string
	ToSimpleLog() string
	GetSourceName() string
}

//go:generate counterfeiter -o fakes/fake_logs_repository.go . LogsRepository
type LogsRepository interface {
	RecentLogsFor(appGuid string) ([]Loggable, error)
	TailLogsFor(appGuid string, onConnect func()) (<-chan Loggable, error)
	Close()
}

const bufferTime time.Duration = 25 * time.Millisecond

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
