name := "elastic-scala"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
"com.sksamuel.elastic4s" %% "elastic4s-core" % "2.1.0",
"com.sksamuel.elastic4s" %% "elastic4s-streams" % "1.7.4",
  "org.slf4j" % "slf4j-simple" % "1.6.4"
)
