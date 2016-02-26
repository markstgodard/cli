package logs_test

import (
	"errors"
	"reflect"

	"github.com/cloudfoundry/cli/cf/configuration/core_config"
	noaa_errors "github.com/cloudfoundry/noaa/errors"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"

	authenticationfakes "github.com/cloudfoundry/cli/cf/api/authentication/fakes"
	testapi "github.com/cloudfoundry/cli/cf/api/logs/fakes"
	testconfig "github.com/cloudfoundry/cli/testhelpers/configuration"

	"github.com/cloudfoundry/cli/cf/api/logs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sync"
)

var _ = Describe("logs with noaa repository", func() {
	var (
		fakeNoaaConsumer   *testapi.FakeNoaaConsumer
		config             core_config.ReadWriter
		fakeTokenRefresher *authenticationfakes.FakeAuthenticationRepository
		repo               *logs.NoaaLogsRepository
	)

	BeforeEach(func() {
		fakeNoaaConsumer = &testapi.FakeNoaaConsumer{}
		config = testconfig.NewRepositoryWithDefaults()
		config.SetLoggregatorEndpoint("loggregator.test.com")
		config.SetDopplerEndpoint("doppler.test.com")
		config.SetAccessToken("the-access-token")
		fakeTokenRefresher = &authenticationfakes.FakeAuthenticationRepository{}
		repo = logs.NewNoaaLogsRepository(config, fakeNoaaConsumer, fakeTokenRefresher)
	})

	Describe("RecentLogsFor", func() {

		It("refreshes token and get metric once more if token has expired.", func() {
			var recentLogsCallCount int

			fakeNoaaConsumer.RecentLogsStub = func(appGuid, authToken string) ([]*events.LogMessage, error) {
				defer func() {
					recentLogsCallCount += 1
				}()

				if recentLogsCallCount == 0 {
					return []*events.LogMessage{}, noaa_errors.NewUnauthorizedError("Unauthorized token")
				}

				return []*events.LogMessage{}, nil
			}

			repo.RecentLogsFor("app-guid")
			Expect(fakeTokenRefresher.RefreshAuthTokenCallCount()).To(Equal(1))
			Expect(fakeNoaaConsumer.RecentLogsCallCount()).To(Equal(2))
		})

		It("refreshes token and get metric once more if token has expired.", func() {
			fakeNoaaConsumer.RecentLogsReturns([]*events.LogMessage{}, errors.New("error error error"))

			_, err := repo.RecentLogsFor("app-guid")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("error error error"))
		})

		Context("when an error does not occur", func() {
			var msg1, msg2, msg3 *events.LogMessage

			BeforeEach(func() {
				msg1 = makeNoaaLogMessage("message 1", 1000)
				msg2 = makeNoaaLogMessage("message 2", 2000)
				msg3 = makeNoaaLogMessage("message 3", 3000)

				fakeNoaaConsumer.RecentLogsReturns([]*events.LogMessage{
					msg3,
					msg2,
					msg1,
				}, nil)
			})

			It("gets the logs for the requested app", func() {
				repo.RecentLogsFor("app-guid-1")
				arg, _ := fakeNoaaConsumer.RecentLogsArgsForCall(0)
				Expect(arg).To(Equal("app-guid-1"))
			})

			It("returns the sorted log messages", func() {
				messages, err := repo.RecentLogsFor("app-guid")
				Expect(err).NotTo(HaveOccurred())

				Expect(messages).To(Equal([]logs.Loggable{
					logs.NewNoaaLogMessage(msg1),
					logs.NewNoaaLogMessage(msg2),
					logs.NewNoaaLogMessage(msg3),
				}))
			})
		})
	})

	Describe("tailing logs", func() {

		Context("when an error occurs", func() {
			It("returns an error when it occurs", func() {
				fakeNoaaConsumer.TailingLogsWithoutReconnectStub = func(appGuid string, authToken string, outputChan chan<- *events.LogMessage) error {
					return errors.New("oops")
				}

				_, err := repo.TailLogsFor("app-guid", func() {})
				Expect(err).To(Equal(errors.New("oops")))
			})
		})

		Context("when a noaa_errors.UnauthorizedError occurs", func() {
			It("refreshes the access token and tail logs once more", func() {
				calledOnce := false
				fakeNoaaConsumer.TailingLogsWithoutReconnectStub = func(appGuid string, authToken string, outputChan chan<- *events.LogMessage) error {
					if !calledOnce {
						calledOnce = true
						return noaa_errors.NewUnauthorizedError("i'm sorry dave")
					} else {
						return errors.New("2nd Error")
					}
				}

				_, err := repo.TailLogsFor("app-guid", func() {})
				Expect(fakeTokenRefresher.RefreshAuthTokenCallCount()).To(Equal(1))
				Expect(err.Error()).To(Equal("2nd Error"))
			})
		})

		Context("when no error occurs", func() {
			It("asks for the logs for the given app", func() {
				fakeNoaaConsumer.TailingLogsWithoutReconnectStub = func(appGuid string, authToken string, outputChan chan<- *events.LogMessage) error {
					return errors.New("quit Tailing")
				}

				repo.TailLogsFor("app-guid", func() {})

				appGuid, token, _ := fakeNoaaConsumer.TailingLogsWithoutReconnectArgsForCall(0)
				Expect(appGuid).To(Equal("app-guid"))
				Expect(token).To(Equal("the-access-token"))
			})

			It("sets the on connect callback", func() {
				fakeNoaaConsumer.TailingLogsWithoutReconnectStub = func(appGuid string, authToken string, outputChan chan<- *events.LogMessage) error {
					return errors.New("quit Tailing")
				}

				var cb = func() { return }
				repo.TailLogsFor("app-guid", cb)

				Expect(fakeNoaaConsumer.SetOnConnectCallbackCallCount()).To(Equal(1))
				arg := fakeNoaaConsumer.SetOnConnectCallbackArgsForCall(0)
				Expect(reflect.ValueOf(arg).Pointer() == reflect.ValueOf(cb).Pointer()).To(BeTrue())
			})
		})

		Context("and the buffer time is sufficient for sorting", func() {
			var msg1, msg2, msg3 *events.LogMessage
			BeforeEach(func() {
				repo = logs.NewNoaaLogsRepository(config, fakeNoaaConsumer, fakeTokenRefresher)
				wait := sync.WaitGroup{}
				msg1 = makeNoaaLogMessage("hello1", 100)
				msg2 = makeNoaaLogMessage("hello2", 200)
				msg3 = makeNoaaLogMessage("hello3", 300)

				fakeNoaaConsumer.TailingLogsWithoutReconnectStub = func(appGuid string, authToken string, outputChan chan<- *events.LogMessage) error {
					wait.Add(1)
					go func() {
						outputChan <- msg3
						outputChan <- msg2
						outputChan <- msg1
						wait.Wait()

						close(outputChan)
					}()

					return nil
				}

				fakeNoaaConsumer.CloseStub = func() error {
					wait.Done()

					return nil
				}
			})

			It("sorts the messages before yielding them", func() {
				receivedMessages := []logs.Loggable{}

				c, err := repo.TailLogsFor("app-guid", func() {})
				Expect(err).NotTo(HaveOccurred())

				for msg := range c {
					receivedMessages = append(receivedMessages, msg)
					if len(receivedMessages) == 3 {
						repo.Close()
					}
				}

				Expect(receivedMessages).To(Equal([]logs.Loggable{
					logs.NewNoaaLogMessage(msg1),
					logs.NewNoaaLogMessage(msg2),
					logs.NewNoaaLogMessage(msg3),
				}))
			})

			It("flushes remaining log messages when Close is called", func() {
				wait := sync.WaitGroup{}
				wait.Add(1)
				receivedMessages := []logs.Loggable{}

				c, err := repo.TailLogsFor("app-guid", func() {})
				Expect(err).NotTo(HaveOccurred())

				go func() {
					for msg := range c {
						receivedMessages = append(receivedMessages, msg)
					}

					wait.Done()
				}()

				repo.Close()
				wait.Wait()

				Expect(receivedMessages).To(Equal([]logs.Loggable{
					logs.NewNoaaLogMessage(msg1),
					logs.NewNoaaLogMessage(msg2),
					logs.NewNoaaLogMessage(msg3),
				}))
			})
		})
	})
})

func makeNoaaLogMessage(message string, timestamp int64) *events.LogMessage {
	messageType := events.LogMessage_OUT
	sourceName := "DEA"
	return &events.LogMessage{
		Message:     []byte(message),
		AppId:       proto.String("app-guid"),
		MessageType: &messageType,
		SourceType:  &sourceName,
		Timestamp:   proto.Int64(timestamp),
	}
}
