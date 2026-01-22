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
    //val jsonString = report.asJson.spaces2
    
    val header= """
    ===============================================
     RAPPORT D'ANALYSE - FILMS & SÉRIES
    =============================================== 
    """

    val total_entry = AnalysisReport.MovieStats.total_movies_parsed
    val total_valid = AnalysisReport.MovieStats.total_movies_valid
    val parsing_errors = AnalysisReport.MovieStats.parsing_errors
    val duplicates_removed = AnalysisReport.MovieStats.duplicates_removed

    val parsing_stats= s"""
    STATISTIQUES DE PARSING
    ---------------------------
    - Entrées totales lues      : {$total_entry}
    - Entrées valides           : {$total_valid}
    - Erreurs de parsing        : {$parsing_errors}
    - Doublons supprimés        : {$duplicates_removed}
    """

    val format_top_10 = formatTopRated(AnalysisReport.top_10_rated)

    val top_10= s"""
    TOP 10 - MEILLEURS FILMS
    ----------------------------
    """ + format_top_10


    val format_top_10_voted = 

    val top_10_voted= s"""
    TOP 10 - PLUS VOTÉS
    -----------------------
    """ 
    
    val writeAttempt = Try {
      Files.write(Paths.get(filename), jsonString.getBytes(StandardCharsets.UTF_8))
  }
    writeAttempt match {
      case scala.util.Success(_) => Right(())
      case scala.util.Failure(exception) => Left(s"Erreur d'écriture du fichier: ${exception.getMessage}")
    }
  }

    private def formatTopRated(movies: List[MovieSummary]): String =
    movies.zipWithIndex
      .map { case (m, idx) =>
        f"${idx + 1}%2d. ${m.title}%-22s : ${m.rating}%.1f/10 (${m.votes} votes)"
      }
      .mkString("\n")

    private def formatTopVoted(movies: List[MovieSummary]): Stro,g = 
    movies.zipWithIndex.map
    {case (m,idx) => f"${idx + 1}%2d. ${m.title} : ${m.rating}%.1f/10 votes"}.mkString("\n")
}