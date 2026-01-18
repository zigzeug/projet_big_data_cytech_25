name := "ex02_data_ingestion"

version := "0.1"

scalaVersion := "2.13.12"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // spark-hadoop-cloud pour compatibilité S3A (AWS S3 et Minio)
  "org.apache.spark" %% "spark-hadoop-cloud" % sparkVersion,
  "org.postgresql" % "postgresql" % "42.7.8"  // Updated to fix CVE-2024-1597
)

// Add JVM options to fix illegal reflective access warnings
run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
run / fork := true
