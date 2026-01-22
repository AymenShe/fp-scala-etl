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
    // TODO: 
    //   1. Convertir en JSON : report.asJson.spaces2
    //   2. Écrire dans le fichier avec Files.write
    //   3. Gérer les erreurs avec Try
    val jsonString = report.asJson.spaces2
    val writeAttempt = Try {
      Files.write(Paths.get(filename), jsonString.getBytes(StandardCharsets.UTF_8))
  }
    writeAttempt match {
      case scala.util.Success(_) => Right(())
      case scala.util.Failure(exception) => Left(s"Erreur d'écriture du fichier: ${exception.getMessage}")
    }
  }
}