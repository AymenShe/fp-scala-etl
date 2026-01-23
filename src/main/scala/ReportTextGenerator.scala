package ETL

import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.util.Try

object ReportTextGenerator {

  def generateReport(movies: List[Movie]): AnalysisReport =
    StatsCalculator.calculateResults(movies)

  def writeReport(report: AnalysisReport, filename: String): Either[String, Unit] = {
    
    val header= """
    ===============================================
     RAPPORT D'ANALYSE - FILMS & SÉRIES
    =============================================== 
    """

    val stats = report.statistics
    val parsing_stats = s"""
    STATISTIQUES DE PARSING
    ---------------------------
    - Entrées totales lues      : ${stats.total_movies_parsed}
    - Entrées valides           : ${stats.total_movies_valid}
    - Erreurs de parsing        : ${stats.parsing_errors}
    - Doublons supprimés        : ${stats.duplicates_removed}
    """

    val top_10: String = s"""
    TOP 10 - MEILLEURS FILMS
    ----------------------------
    """ + formatTopRated(report.top_10_rated)

    val top_10_voted: String = s"""
    TOP 10 - PLUS VOTÉS
    -----------------------
    """ + formatTopVoted(report.top_10_by_votes)
    
    val top_10_boxoffice = s"""
    TOP 10 - BOX-OFFICE
    -----------------------
    """ + formatBoxOffice(report.highest_grossing)

    val top_10_budget: String = s"""
    TOP 10 - BUDGET
    -----------------------
    """ + formatBudget(report.most_expensive)

    val top_10_decade: String = s"""
    RÉPARTITION PAR DÉCENNIE
    ----------------------------
    """ + formatDecade(report.movies_by_decade)

    val top_10_genre: String = s"""
    RÉPARTITION PAR GENRE
    -------------------------
    """ + formatGenre(report.movies_by_genre)

    val top_10_average_rating: String = s"""
    NOTE MOYENNE PAR GENRE
    -----------------------
    """ + formatAverageRating(report.average_rating_by_genre)

    val top_10_average_runtime: String = s"""
    DURÉE MOYENNE PAR GENRE
    -----------------------
    """ + formatAverageRuntime(report.average_runtime_by_genre)

    val top_10_most_prolific_directors: String = s"""
    TOP 5 - RÉALISATEURS
    ------------------------
    """ + formatMostProlificDirectors(report.most_prolific_directors)

    val top_10_most_frequent_actors: String = s"""
    TOP 5 - ACTEURS
    -------------------
    """ + formatMostFrequentActors(report.most_frequent_actors)

    val top_10_profitable_movies: String = s"""
    RENTABILITÉ
    --------------
    """ + formatProfitableMovies(report.profitable_movies)

    val reportFinal: String = s"""
    $header
    $parsing_stats
    $top_10
    $top_10_voted
    $top_10_boxoffice
    $top_10_budget
    $top_10_decade
    $top_10_genre
    $top_10_average_rating
    $top_10_average_runtime
    $top_10_most_prolific_directors
    $top_10_most_frequent_actors
    $top_10_profitable_movies
    """

    val writeAttempt = Try {
      Files.write(Paths.get(filename), reportFinal.getBytes(StandardCharsets.UTF_8))
    }
    writeAttempt match {
      case scala.util.Success(_) => Right(())
      case scala.util.Failure(exception) => Left(s"Erreur d'écriture du fichier: ${exception.getMessage}")
    }
  }

  private def formatTopRated(movies: Seq[MovieSummary]): String =
    movies.zipWithIndex
      .map { case (m, idx) =>
        f"${idx + 1}%2d. ${m.title}%-25s : ${m.rating}%.1f/10 (${m.votes} votes)"
      }
      .mkString("\n")

  private def formatTopVoted(movies: Seq[MovieSummary]): String = 
    movies.zipWithIndex.map { case (m, idx) => 
      f"${idx + 1}%2d. ${m.title}%-25s : ${m.votes} votes"
    }.mkString("\n")

  private def formatBoxOffice(movies: Seq[MovieGrossingSummary]): String = 
    movies.zipWithIndex.map { case (m, idx) => 
      f"${idx + 1}%2d. ${m.title}%-25s : ${m.revenue}%.1f M$$"
    }.mkString("\n")

  private def formatBudget(movies: Seq[MovieBudgetSummary]): String = 
    movies.zipWithIndex.map { case (m, idx) => 
      f"${idx + 1}%2d. ${m.title}%-25s : ${m.budget}%.1f M$$"
    }.mkString("\n")

  private def formatDecade(decades: Map[String, Int]): String = 
    decades.toSeq.sortBy(_._1).map { case (decade, count) => 
      s"- $decade : $count films"
    }.mkString("\n")

  private def formatGenre(genres: Map[String, Int]): String = 
    genres.toSeq.sortBy(-_._2).map { case (genre, count) => 
      s"- $genre : $count films"
    }.mkString("\n")

  private def formatAverageRating(ratings: Map[String, Double]): String = 
    ratings.toSeq.sortBy(-_._2).map { case (genre, rating) => 
      f"- $genre%-15s : $rating%.1f/10"
    }.mkString("\n")

  private def formatAverageRuntime(runtimes: Map[String, Double]): String = 
    runtimes.toSeq.sortBy(-_._2).map { case (genre, runtime) => 
      f"- $genre%-15s : $runtime%.0f minutes"
    }.mkString("\n")

  private def formatMostProlificDirectors(directors: Seq[ProlificDirector]): String = 
    directors.zipWithIndex.map { case (d, idx) => 
      f"${idx + 1}%2d. ${d.director}%-25s : ${d.count} films"
    }.mkString("\n")

  private def formatMostFrequentActors(actors: Seq[FrequentActor]): String = 
    actors.zipWithIndex.map { case (a, idx) => 
      f"${idx + 1}%2d. ${a.actor}%-25s : ${a.count} films"
    }.mkString("\n")

  private def formatProfitableMovies(stats: ProfitableMovies): String = {
    s"""- Films rentables           : ${stats.count} films
    - ROI moyen                 : ${f"${stats.average_roi}%.2f"}x"""
  }
}