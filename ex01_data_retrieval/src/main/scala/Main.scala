import org.apache.spark.sql.SparkSession
import java.net.URL
import java.io.{File, FileOutputStream}
import java.nio.channels.Channels
import org.apache.log4j.{Logger, LogManager}

/** Data Retrieval - Téléchargement des données NYC Yellow Taxi Récupère les
  * fichiers parquet depuis le site officiel TLC
  */
object Main {
  private val logger: Logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DataRetrieval")
      .master("local[*]")
      .getOrCreate()

    try {
      // Configuration
      val dataUrl =
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
      val outputDir = "../data/raw"
      val outputFile = s"$outputDir/yellow_tripdata_2025-01.parquet"

      // Création du dossier de destination
      val dir = new File(outputDir)
      if (!dir.exists()) {
        dir.mkdirs()
        logger.info(s"Créé le dossier: $outputDir")
      }

      // Téléchargement du fichier
      val file = new File(outputFile)
      if (!file.exists()) {
        logger.info(s"Téléchargement depuis: $dataUrl")
        downloadFile(dataUrl, outputFile)
        logger.info(s"Fichier sauvegardé: $outputFile")
      } else {
        logger.info(s"Le fichier existe déjà: $outputFile")
      }

      // Vérification rapide avec Spark
      val df = spark.read.parquet(outputFile)
      logger.info(
        s"Données chargées avec succès - ${df.count()} enregistrements"
      )
      logger.info(s"Colonnes: ${df.columns.mkString(", ")}")

    } catch {
      case e: Exception =>
        logger.error(s"Erreur: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /** Télécharge un fichier depuis une URL
    */
  private def downloadFile(urlString: String, outputPath: String): Unit = {
    val url = new URL(urlString)
    val connection = url.openConnection()
    connection.setRequestProperty("User-Agent", "Mozilla/5.0")

    val inputStream = connection.getInputStream
    val readableByteChannel = Channels.newChannel(inputStream)
    val fileOutputStream = new FileOutputStream(outputPath)

    try {
      fileOutputStream.getChannel.transferFrom(
        readableByteChannel,
        0,
        Long.MaxValue
      )
    } finally {
      readableByteChannel.close()
      fileOutputStream.close()
      inputStream.close()
    }
  }
}
