name := "hat-tool"

scalaVersion := "2.11.7"


libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.rogach" %% "scallop" % "0.9.5"


libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.0"
libraryDependencies +=  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.0-1"
libraryDependencies +=  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.6.0"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.12"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.12" % "runtime,optional"

libraryDependencies += "com.ning" % "async-http-client" % "1.9.21"
libraryDependencies += "commons-codec" % "commons-codec" % "1.10"

libraryDependencies += "org.specs2" %% "specs2-core" % "3.6.6" % "test"
