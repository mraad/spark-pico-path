organization := "com.esri"

name := "spark-pico-path"

version := "0.1"

scalaVersion := "2.11.8"

isSnapshot := true

val sparkVersion = "2.3.0"

publishMavenStyle := true

resolvers += Resolver.mavenLocal

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "joda-time" % "joda-time" % "2.9.9" % "provided"
libraryDependencies += "org.joda" % "joda-convert" % "1.9.2" % "provided"
libraryDependencies += "com.esri" %% "filegdb" % "0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
