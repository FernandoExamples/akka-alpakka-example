import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, Materializer }
import akka.stream.alpakka.amqp.scaladsl.{ AmqpFlow, AmqpSink, AmqpSource }
import akka.stream.alpakka.amqp.{
  AmqpCachedConnectionProvider,
  AmqpConnectionProvider,
  AmqpUriConnectionProvider,
  AmqpWriteSettings,
  NamedQueueSourceSettings,
  QueueDeclaration,
  TemporaryQueueSourceSettings,
  WriteMessage
}
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.ByteString
import com.typesafe.config.Config
import configs.{ GlobalConfig, RabbitMQConfig }
import utils.LogSupport

import scala.concurrent.duration.DurationInt

abstract class Runtime {
  val globalConfig: GlobalConfig
  val rabbitMQConfig: RabbitMQConfig
}

object Runtime extends LogSupport {

  def apply(config: Config): Runtime =
    new Runtime {
      override val globalConfig: GlobalConfig = GlobalConfig(config.getString("value1"), config.getString("value2"))

      override val rabbitMQConfig: RabbitMQConfig =
        RabbitMQConfig(url = "amqp://localhost", queueName = "amqp-example", exchangeName = "exchange-example")

    }

  def execute(runtime: Runtime): Unit = {
    logger.info("Starting Runtime")
    logger.info(s"Configs: ${runtime.globalConfig}")

    //Example of writing messages via Flow
    implicit val system = ActorSystem("AMQP")

    val connectionProvider = runtime.rabbitMQConfig.connectionProvider

    val settings = AmqpWriteSettings(connectionProvider)
      .withRoutingKey(runtime.rabbitMQConfig.queueName)
      .withDeclaration(runtime.rabbitMQConfig.queueDeclaration)
      .withBufferSize(10)
      .withConfirmationTimeout(200.millis)

    val amqpFlow = AmqpFlow.withConfirm(settings)

    val input = Vector("one", "two", "three", "four", "five")

    val result = Source(input).map(message => WriteMessage(ByteString(message))).via(amqpFlow).runWith(Sink.seq)

    //The same example but with Sink
    val amqpSink = AmqpSink.simple(
      AmqpWriteSettings(runtime.rabbitMQConfig.connectionProvider)
        .withRoutingKey(runtime.rabbitMQConfig.queueName)
        .withDeclaration(runtime.rabbitMQConfig.queueDeclaration)
        .withConfirmationTimeout(200.millis)
        .withBufferSize(10)
    )

    val writing = Source(input).log("Source").map(message => ByteString(message)).runWith(amqpSink)

    //Leer mensajes de una cola
    val suscribedSource = AmqpSource.atMostOnceSource(
      NamedQueueSourceSettings(runtime.rabbitMQConfig.connectionProvider, runtime.rabbitMQConfig.queueName)
        .withDeclaration(runtime.rabbitMQConfig.queueDeclaration)
        .withAckRequired(true),
      bufferSize = 10
    )

    suscribedSource
      .map(message => message.bytes.utf8String)
      .runWith(Sink.foreach(content => logger.info(s"Mensaje leido $content")))

    //Trabajando con Exchanges
    val amqpSinkWithExchange = AmqpSink.simple(
      AmqpWriteSettings(runtime.rabbitMQConfig.connectionProvider)
        .withExchange(runtime.rabbitMQConfig.exchangeName)
        .withDeclaration(runtime.rabbitMQConfig.exchangeDeclaration))

    val mergedSources = (0 until 4).foldLeft(Source.empty[(Int, String)]) { case (source, fanoutBranch) =>
      source.merge(
        AmqpSource
          .atMostOnceSource(
            TemporaryQueueSourceSettings(
              runtime.rabbitMQConfig.connectionProvider,
              runtime.rabbitMQConfig.exchangeName
            ).withDeclaration(runtime.rabbitMQConfig.exchangeDeclaration),
            bufferSize = 1
          )
          .map(msg => (fanoutBranch, msg.bytes.utf8String))
      )
    }

    //Commitable Source (Ack)
    val suscribedCommitableSource = AmqpSource.committableSource(
      NamedQueueSourceSettings(runtime.rabbitMQConfig.connectionProvider, runtime.rabbitMQConfig.queueName)
        .withDeclaration(runtime.rabbitMQConfig.queueDeclaration)
        .withAckRequired(true),
      bufferSize = 10
    )

    import system.dispatcher

    suscribedCommitableSource
      .map(x => {
        println(s"Mensaje que requiere ack: ${x.message.bytes.utf8String}")
        x.ack().map(_ => x.message.bytes.utf8String)
      })
      .runWith(Sink.foreach(content => content.map(message => logger.info(s"Mensaje leido $message"))))

  }

}
