package logs_test

import (
	authenticationfakes "github.com/cloudfoundry/cli/cf/api/authentication/fakes"
	testapi "github.com/cloudfoundry/cli/cf/api/logs/fakes"
	"github.com/cloudfoundry/cli/cf/configuration/core_config"
	"github.com/cloudfoundry/cli/cf/errors"
	testconfig "github.com/cloudfoundry/cli/testhelpers/configuration"
	"github.com/cloudfoundry/loggregatorlib/logmessage"
	noaa_errors "github.com/cloudfoundry/noaa/errors"
	"github.com/gogo/protobuf/proto"

	. "github.com/cloudfoundry/cli/cf/api/logs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("loggregator logs repository", func() {
	var (
		fakeConsumer *testapi.FakeLoggregatorConsumer
		logsRepo     LogsRepository
		configRepo   core_config.ReadWriter
		authRepo     *authenticationfakes.FakeAuthenticationRepository
	)

	BeforeEach(func() {
		fakeConsumer = testapi.NewFakeLoggregatorConsumer()
		configRepo = testconfig.NewRepositoryWithDefaults()
		configRepo.SetLoggregatorEndpoint("loggregator-server.test.com")
		configRepo.SetAccessToken("the-access-token")
		authRepo = &authenticationfakes.FakeAuthenticationRepository{}
	})

	JustBeforeEach(func() {
		logsRepo = NewLoggregatorLogsRepository(configRepo, fakeConsumer, authRepo)
	})

	Describe("RecentLogsFor", func() {
		Context("when a noaa_errors.UnauthorizedError occurs", func() {
			BeforeEach(func() {
				fakeConsumer.RecentReturns.Err = []error{
					noaa_errors.NewUnauthorizedError("i'm sorry dave"),
					nil,
				}
			})

			It("refreshes the access token", func() {
				_, err := logsRepo.RecentLogsFor("app-guid")
				Expect(err).ToNot(HaveOccurred())
				Expect(authRepo.RefreshAuthTokenCallCount()).To(Equal(1))
			})
		})

		Context("when an error occurs", func() {
			BeforeEach(func() {
				fakeConsumer.RecentReturns.Err = []error{errors.New("oops")}
			})

			It("returns the error", func() {
				_, err := logsRepo.RecentLogsFor("app-guid")
				Expect(err).To(Equal(errors.New("oops")))
			})
		})

		Context("when an error does not occur", func() {
			var msg1, msg2 *logmessage.LogMessage

			BeforeEach(func() {
				msg1 = makeLogMessage("My message 2", int64(2000))
				msg2 = makeLogMessage("My message 1", int64(1000))

				fakeConsumer.RecentReturns.Messages = []*logmessage.LogMessage{
					msg1,
					msg2,
				}
			})

			It("gets the logs for the requested app", func() {
				logsRepo.RecentLogsFor("app-guid")
				Expect(fakeConsumer.RecentCalledWith.AppGuid).To(Equal("app-guid"))
			})

			It("writes the sorted log messages onto the provided channel", func() {
				messages, err := logsRepo.RecentLogsFor("app-guid")
				Expect(err).NotTo(HaveOccurred())

				Expect(messages).To(Equal([]Loggable{
					NewLoggregatorLogMessage(msg2),
					NewLoggregatorLogMessage(msg1),
				}))
			})
		})
	})

	Describe("tailing logs", func() {
		var logChan chan Loggable
		var errChan chan error

		BeforeEach(func() {
			logChan = make(chan Loggable)
			errChan = make(chan error)
		})

		Context("when an error occurs", func() {
			e := errors.New("oops")

			BeforeEach(func() {
				fakeConsumer.TailFunc = func(_, _ string) (<-chan *logmessage.LogMessage, error) {
					return nil, e
				}
			})

			It("returns an error", func(done Done) {
				go func() {
					Eventually(errChan).Should(Receive(&e))

					close(done)
				}()

				logsRepo.TailLogsFor("app-guid", func() {}, logChan, errChan)
			})
		})

		Context("when a LoggregatorConsumer.UnauthorizedError occurs", func() {
			It("refreshes the access token", func(done Done) {
				calledOnce := false

				fakeConsumer.TailFunc = func(_, _ string) (<-chan *logmessage.LogMessage, error) {
					if !calledOnce {
						calledOnce = true
						return nil, noaa_errors.NewUnauthorizedError("i'm sorry dave")
					} else {
						return nil, nil
					}
				}

				go func() {
					defer GinkgoRecover()

					Eventually(authRepo.RefreshAuthTokenCallCount()).Should(Equal(1))
					Consistently(errChan).ShouldNot(Receive())

					close(done)
				}()

				logsRepo.TailLogsFor("app-guid", func() {}, logChan, errChan)
			})

			Context("when LoggregatorConsumer.UnauthorizedError occurs again", func() {
				It("returns an error", func(done Done) {
					err := noaa_errors.NewUnauthorizedError("All the errors")

					fakeConsumer.TailFunc = func(_, _ string) (<-chan *logmessage.LogMessage, error) {
						return nil, err
					}

					go func() {
						defer GinkgoRecover()

						// Not equivalent to ShouldNot(Receive(BeNil()))
						// Should receive something, but it shouldn't be nil
						Eventually(errChan).Should(Receive(&err))
						close(done)
					}()

					logsRepo.TailLogsFor("app-guid", func() {}, logChan, errChan)
				})
			})
		})

		Context("when no error occurs", func() {
			It("asks for the logs for the given app", func() {
				fakeConsumer.TailFunc = func(appGuid, token string) (<-chan *logmessage.LogMessage, error) {
					Expect(appGuid).To(Equal("app-guid"))
					Expect(token).To(Equal("the-access-token"))
					return nil, nil
				}

				logsRepo.TailLogsFor("app-guid", func() {}, logChan, errChan)
			})

			It("sets the on connect callback", func() {
				fakeConsumer.TailFunc = func(_, _ string) (<-chan *logmessage.LogMessage, error) {
					return nil, nil
				}

				called := false
				logsRepo.TailLogsFor("app-guid", func() { called = true }, logChan, errChan)
				fakeConsumer.OnConnectCallback()
				Expect(called).To(BeTrue())
			})

			It("sorts the messages before yielding them", func(done Done) {
				var receivedMessages []Loggable
				msg3 := makeLogMessage("hello3", 300)
				msg2 := makeLogMessage("hello2", 200)
				msg1 := makeLogMessage("hello1", 100)

				fakeConsumer.TailFunc = func(_, _ string) (<-chan *logmessage.LogMessage, error) {
					logChan := make(chan *logmessage.LogMessage)
					go func() {
						logChan <- msg3
						logChan <- msg2
						logChan <- msg1
						fakeConsumer.WaitForClose()
						close(logChan)
					}()

					return logChan, nil
				}

				logsRepo.TailLogsFor("app-guid", func() {}, logChan, errChan)

				for msg := range logChan {
					receivedMessages = append(receivedMessages, msg)
					if len(receivedMessages) >= 3 {
						logsRepo.Close()
					}
				}

				Eventually(errChan).ShouldNot(Receive())

				Expect(receivedMessages).To(Equal([]Loggable{
					NewLoggregatorLogMessage(msg1),
					NewLoggregatorLogMessage(msg2),
					NewLoggregatorLogMessage(msg3),
				}))

				close(done)
			})

			It("flushes remaining log messages and closes the returned channel when Close is called", func(done Done) {
				var receivedMessages []Loggable
				msg3 := makeLogMessage("hello3", 300)
				msg2 := makeLogMessage("hello2", 200)
				msg1 := makeLogMessage("hello1", 100)

				fakeConsumer.TailFunc = func(_, _ string) (<-chan *logmessage.LogMessage, error) {
					messageChan := make(chan *logmessage.LogMessage)
					go func() {
						messageChan <- msg3
						messageChan <- msg2
						messageChan <- msg1
						fakeConsumer.WaitForClose()
						close(messageChan)
					}()

					return messageChan, nil
				}

				Expect(fakeConsumer.IsClosed).To(BeFalse())

				go func() {
					for msg := range logChan {
						receivedMessages = append(receivedMessages, msg)
					}
					close(done)
				}()

				logsRepo.TailLogsFor("app-guid", func() {}, logChan, errChan)
				Expect(errChan).NotTo(Receive())
				Expect(logChan).NotTo(BeClosed())

				logsRepo.Close()

				Expect(fakeConsumer.IsClosed).To(BeTrue())

				Eventually(logChan).Should(BeClosed())
				Eventually(errChan).Should(BeClosed())

				Expect(receivedMessages).To(Equal([]Loggable{
					NewLoggregatorLogMessage(msg1),
					NewLoggregatorLogMessage(msg2),
					NewLoggregatorLogMessage(msg3),
				}))
			})
		})
	})
})

func makeLogMessage(message string, timestamp int64) *logmessage.LogMessage {
	messageType := logmessage.LogMessage_OUT
	sourceName := "DEA"
	return &logmessage.LogMessage{
		Message:     []byte(message),
		AppId:       proto.String("my-app-guid"),
		MessageType: &messageType,
		SourceName:  &sourceName,
		Timestamp:   proto.Int64(timestamp),
	}
}
