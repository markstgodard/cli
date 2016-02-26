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

func (repo *NoaaLogsRepository) TailLogsFor(appGuid string, onConnect func()) (<-chan Loggable, error) {
	ticker := time.NewTicker(bufferTime)
	c := make(chan Loggable)
	logChan := make(chan *events.LogMessage)

	endpoint := repo.config.DopplerEndpoint()
	if endpoint == "" {
		return nil, errors.New(T("Loggregator endpoint missing from config file"))
	}

	repo.consumer.SetOnConnectCallback(onConnect)

	go func() {
		err := repo.consumer.TailingLogsWithoutReconnect(appGuid, repo.config.AccessToken(), logChan)
		switch err.(type) {
		case nil:
		case *noaa_errors.UnauthorizedError:
			repo.tokenRefresher.RefreshAuthToken()
			repo.consumer.TailingLogsWithoutReconnect(appGuid, repo.config.AccessToken(), logChan)
		default:
			repo.consumer.Close()
		}

	}()

	go func() {
		for _ = range ticker.C {
			repo.flushMessageQueue(c)
		}
	}()

	go func() {
		for msg := range logChan {
			repo.messageQueue.PushMessage(msg)
		}

		repo.flushMessageQueue(c)
		close(c)
	}()

	return c, nil
}

func (repo *NoaaLogsRepository) flushMessageQueue(c chan Loggable) {
	repo.messageQueue.EnumerateAndClear(func(m *events.LogMessage) {
		c <- NewNoaaLogMessage(m)
	})
}
