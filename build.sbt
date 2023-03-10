ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "ProjetBigData_V4"
  )

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.3.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.2" %"provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.3.1"
libraryDependencies += "org.apache.derby" % "derby" % "10.12.1.1"
libraryDependencies += "org.apache.derby" % "derbyclient" % "10.12.1.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.4"
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "3.1.3"
libraryDependencies += "org.apache.hive" % "hive-metastore" % "3.1.3"









