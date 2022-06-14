ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"
lazy val AkkaVersion = "2.6.19"
lazy val AkkaHttpVersion = "10.2.9"

lazy val root = (project in file("."))
  .settings(
    name := "alpakka-example"
  )

libraryDependencies ++= Seq(
  // akka actors
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  //akka http
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
  // akka streams
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  //Akka AMQP
  "com.lightbend.akka" %% "akka-stream-alpakka-amqp" % "3.0.4",
  //Test
  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.12" % Test,
  //Logging
  "ch.qos.logback" % "logback-classic" % "1.2.11"
)
