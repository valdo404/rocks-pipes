name := "rocksdb"

version := "0.1"
scalaVersion := "2.12.8"
resolvers += "Sonatype Public" at "https://oss.sonatype.org/content/groups/public/"

libraryDependencies += "org.rocksdb" % "rocksdbjni" % "6.0.1"
libraryDependencies += "org.typelevel" %% "cats-effect" % "1.3.1"
libraryDependencies += "co.fs2" %% "fs2-core" % "1.0.4"
libraryDependencies += "org.scodec" %% "scodec-stream" % "1.0.1"
libraryDependencies += "org.scodec" %% "scodec-core" % "1.10.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"


libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3" % "test"

