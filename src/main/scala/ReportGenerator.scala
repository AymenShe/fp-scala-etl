package ETL

import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.util.Try

object ReportGenerator {

  /**
   * Génère le rapport complet
   */
  def generateReport(movies: List[Movie]): AnalysisReport =
    StatsCalculator.calculateResults(movies)

  /**
   * Écrit le rapport en JSON
   */
  def writeReport(report: AnalysisReport, filename: String): Either[String, Unit] = {
    val jsonString = report.asJson.spaces2
    val writeAttempt = Try {
      val path = Paths.get(filename)
      val parent = Option(path.getParent)
      parent.foreach(p => Files.createDirectories(p))
      Files.write(path, jsonString.getBytes(StandardCharsets.UTF_8))
  }
    writeAttempt match {
      case scala.util.Success(_) => Right(())
      case scala.util.Failure(exception) => Left(s"Erreur d'écriture du fichier: ${exception.getMessage}")
    }
  }
}