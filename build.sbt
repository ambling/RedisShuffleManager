
name := "RedisShuffler"

version := "1.0"

scalaVersion := "2.11.7"

val hadoopVersion = "2.7.1"

val sparkVersion = "2.1.0"

licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided" exclude("com.google.guava", "guava") exclude("org.slf4j", "slf4j-log4j12")

libraryDependencies += "redis.clients" % "jedis" % "2.9.0"