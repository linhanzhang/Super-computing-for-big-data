name := "Lab 1"
version := "1.0"
scalaVersion := "2.12.14"

scalastyleFailOnWarning := true

fork in run := true

val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // https://mvnrepository.com/artifact/com.uber/h3
  "com.uber" % "h3" % "3.7.0"
)

// https://mvnrepository.com/artifact/com.uber/h3
//libraryDependencies += "com.uber" % "h3" % "3.7.0"

