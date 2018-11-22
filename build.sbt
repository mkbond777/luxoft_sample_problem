name := "luxoft_sample_problem"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.4.0")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"

parallelExecution in Test := false

fork in Test := true