package internal

type ConsumerGroupConsumers struct {
	Consumer                 Consumer
	BatchConsumer            BatchConsumer
	ErrorConsumer            Consumer
	ConsumerErrorInterceptor ConsumerErrorInterceptor
	ConfigName               string
	ConsumerInterceptors     []ConsumerInterceptor
}

type ConsumerGroupErrorConsumers struct {
	ErrorConsumer            Consumer
	ConsumerErrorInterceptor ConsumerErrorInterceptor
	ConfigName               string
}
