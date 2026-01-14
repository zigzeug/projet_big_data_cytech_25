import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Logger, LogManager}
import java.io.File
import java.util.Properties

/** Data Validation & Ingestion
  *   - Valide les données parquet
  *   - Stocke dans S3/Minio via l'API S3A (compatible avec les deux)
  *   - Ingère dans PostgreSQL
  */
object DataValidation {
  private val logger: Logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // Configuration S3A via variables d'environnement
    val s3Endpoint = sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000")
    val s3AccessKey = sys.env.getOrElse(
      "S3_ACCESS_KEY",
      sys.env.getOrElse("MINIO_ACCESS_KEY", "")
    )
    val s3SecretKey = sys.env.getOrElse(
      "S3_SECRET_KEY",
      sys.env.getOrElse("MINIO_SECRET_KEY", "")
    )
    val bucketName = sys.env.getOrElse("S3_BUCKET", "warehouse")

    val spark = SparkSession
      .builder()
      .appName("DataIngestion")
      .master("local[*]")
      // Configuration Hadoop S3A - compatible AWS S3 ET Minio
      .config("spark.hadoop.fs.s3a.endpoint", s3Endpoint)
      .config("spark.hadoop.fs.s3a.access.key", s3AccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", s3SecretKey)
      .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
      ) // Nécessaire pour Minio
      .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
      )
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    // Lecture des données parquet
    val inputPath = "../data/raw/yellow_tripdata_2025-01.parquet"
    val s3OutputPath = s"s3a://$bucketName/validated/yellow_tripdata_2025-01"

    logger.info(s"Lecture depuis $inputPath")

    try {
      val df = spark.read.parquet(inputPath)
      logger.info(s"Total records: ${df.count()}")

      // Filtrage des données invalides
      val validatedDf = df.filter(
        col("passenger_count") > 0 &&
          col("trip_distance") >= 0 &&
          col("total_amount") >= 0 &&
          col("tpep_pickup_datetime") < col("tpep_dropoff_datetime")
      )

      val validCount = validatedDf.count()
      logger.info(s"Records valides: $validCount")

      // --- Etape 1: Stockage S3/Minio via S3A ---
      logger.info("--- Branch 1: Stockage S3/Minio (via S3A) ---")

      validatedDf.write
        .mode(SaveMode.Overwrite)
        .parquet(s3OutputPath)

      logger.info(s"Upload S3A terminé: $s3OutputPath")

      // --- Etape 2: Ingestion Postgres ---
      logger.info("--- Branch 2: Ingestion Postgres ---")

      // Mapping vers le schéma fact_trips
      val postgresDf = validatedDf.select(
        col("VendorID").as("vendor_id"),
        col("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime"),
        col("passenger_count"),
        col("trip_distance"),
        col("RatecodeID").as("rate_code_id"),
        col("store_and_fwd_flag"),
        col("PULocationID").as("pulocation_id"),
        col("DOLocationID").as("dolocation_id"),
        col("payment_type").as("payment_type_id"),
        col("fare_amount"),
        col("extra"),
        col("mta_tax"),
        col("tip_amount"),
        col("tolls_amount"),
        col("improvement_surcharge"),
        col("total_amount"),
        col("congestion_surcharge"),
        col("Airport_fee").as("airport_fee")
      )

      // Configuration JDBC via Env Vars
      val dbHost = sys.env.getOrElse("DB_HOST", "localhost")
      val dbPort = sys.env.getOrElse("DB_PORT", "5433")
      val dbName = sys.env.getOrElse("DB_NAME", "warehouse")
      val dbUser = sys.env.getOrElse("DB_USER", "postgres")
      val dbPass = sys.env.getOrElse("DB_PASS", "password")

      val jdbcUrl = s"jdbc:postgresql://$dbHost:$dbPort/$dbName"
      val connectionProperties = new Properties()
      connectionProperties.put("user", dbUser)
      connectionProperties.put("password", dbPass)
      connectionProperties.put("driver", "org.postgresql.Driver")

      // Ecriture en base
      logger.info(s"Ecriture dans Postgres ($jdbcUrl)...")
      logger.info(s"Postgres DataFrame count: ${postgresDf.count()}")

      postgresDf.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "fact_trips", connectionProperties)

      logger.info("Pipeline terminé avec succès.")

    } catch {
      case e: Exception =>
        logger.error(s"Erreur: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
