ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "ex01_data_retrieval"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.5"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.5"
// spark-hadoop-cloud inclut hadoop-aws et le SDK AWS - plus simple et léger
// https://mvnrepository.com/artifact/org.apache.spark/spark-hadoop-cloud
libraryDependencies += "org.apache.spark" %% "spark-hadoop-cloud" % "3.5.5"

// Without forking, ctrl-c doesn't actually fully stop Spark
run / fork := true
Test / fork := true

// Add JVM options to fix illegal reflective access warnings
run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
