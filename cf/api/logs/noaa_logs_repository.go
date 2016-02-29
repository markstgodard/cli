package logs

import (
	"errors"
	"time"

	. "github.com/cloudfoundry/cli/cf/i18n"

	"github.com/cloudfoundry/cli/cf/api/authentication"
	"github.com/cloudfoundry/cli/cf/configuration/core_config"

	"github.com/cloudfoundry/noaa"
	noaa_errors "github.com/cloudfoundry/noaa/errors"
	"github.com/cloudfoundry/sonde-go/events"
)

type NoaaLogsRepository struct {
	config         core_config.Reader
	consumer       NoaaConsumer
	tokenRefresher authentication.TokenRefresher
	messageQueue   *NoaaMessageQueue
}

func NewNoaaLogsRepository(config core_config.Reader, consumer NoaaConsumer, tr authentication.TokenRefresher) *NoaaLogsRepository {
	return &NoaaLogsRepository{
		config:         config,
		consumer:       consumer,
		tokenRefresher: tr,
		messageQueue:   NewNoaaMessageQueue(),
	}
}

func (repo *NoaaLogsRepository) Close() {
	repo.consumer.Close()
}

func loggableMessagesFromNoaaMessages(messages []*events.LogMessage) []Loggable {
	loggableMessages := make([]Loggable, len(messages))

	for i, m := range messages {
		loggableMessages[i] = NewNoaaLogMessage(m)
	}

	return loggableMessages
}

func (repo *NoaaLogsRepository) RecentLogsFor(appGuid string) ([]Loggable, error) {
	logs, err := repo.consumer.RecentLogs(appGuid, repo.config.AccessToken())

	switch err.(type) {
	case nil: // do nothing
	case *noaa_errors.UnauthorizedError:
		repo.tokenRefresher.RefreshAuthToken()
		return repo.RecentLogsFor(appGuid)
	default:
		return loggableMessagesFromNoaaMessages(logs), err
	}

	return loggableMessagesFromNoaaMessages(noaa.SortRecent(logs)), err
}

func (repo *NoaaLogsRepository) TailLogsFor(appGuid string, onConnect func(), logChan chan<- Loggable, errChan chan<- error) {
	ticker := time.NewTicker(bufferTime)
	c := make(chan *events.LogMessage)
	e := make(chan error)

	endpoint := repo.config.DopplerEndpoint()
	if endpoint == "" {
		errChan <- errors.New(T("Loggregator endpoint missing from config file"))
		close(errChan)
		close(logChan)
		return
	}

	repo.consumer.SetOnConnectCallback(onConnect)

	go func() {
		for {
			select {
			case msg, ok := <-c:
				if !ok {
					repo.flushMessageQueue(logChan)
					close(errChan)
					close(logChan)
					return
				}

				repo.messageQueue.PushMessage(msg)
			case err := <-e:
				switch err.(type) {
				case nil:
				case *noaa_errors.UnauthorizedError:
					repo.tokenRefresher.RefreshAuthToken()
					repo.TailLogsFor(appGuid, onConnect, logChan, errChan)
				default:
					errChan <- err
					return
				}
			}
		}
	}()

	go func() {
		for _ = range ticker.C {
			repo.flushMessageQueue(logChan)
		}
	}()

	go func() {
		repo.consumer.TailingLogs(appGuid, repo.config.AccessToken(), c, e)
	}()
}

func (repo *NoaaLogsRepository) flushMessageQueue(c chan<- Loggable) {
	repo.messageQueue.EnumerateAndClear(func(m *events.LogMessage) {
		c <- NewNoaaLogMessage(m)
	})
}
