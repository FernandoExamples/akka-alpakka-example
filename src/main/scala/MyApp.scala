import com.typesafe.config.ConfigFactory
import utils.LogSupport

object MyApp extends App with LogSupport {

  lazy val runtime = Runtime(ConfigFactory.load())
  Runtime.execute(runtime)
}
