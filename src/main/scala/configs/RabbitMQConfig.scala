package configs

import akka.stream.alpakka.amqp.{AmqpCachedConnectionProvider, AmqpConnectionProvider, AmqpUriConnectionProvider, ExchangeDeclaration, QueueDeclaration}

case class RabbitMQConfig(url: String, queueName: String, exchangeName: String) {

  val queueDeclaration: QueueDeclaration = QueueDeclaration(queueName).withDurable(true)

  val exchangeDeclaration: ExchangeDeclaration =
    ExchangeDeclaration(exchangeName, "direct").withDurable(true)

  val connectionProvider: AmqpConnectionProvider = AmqpCachedConnectionProvider(AmqpUriConnectionProvider(url))

}
