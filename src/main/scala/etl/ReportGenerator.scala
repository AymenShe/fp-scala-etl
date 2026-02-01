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
  def generateReport(load: LoadResult): AnalysisReport =
    StatsCalculator.calculateResults(load)

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

  private def safeDiv(n: Double, d: Double): Double =
    if (d == 0.0) 0.0 else n / d

  private def formatMoneyM(d: Double): String = f"$d%.2f M$$"

  /**
   * Génère un rapport texte
   */
  def generateTextReport(load: LoadResult, report: AnalysisReport, durationSeconds: Double): String = {
    val sb = new StringBuilder

    val stats = report.statistics
    val entriesPerSecond = safeDiv(stats.total_movies_parsed.toDouble, durationSeconds)

    val topBudgets = StatsCalculator.topBudgets(load.movies, 10)
    val bestRoi = StatsCalculator.bestRoi(load.movies)

    sb.append("===============================================\n")
    sb.append("     RAPPORT D'ANALYSE - FILMS & SÉRIES\n")
    sb.append("===============================================\n\n")

    sb.append("STATISTIQUES DE PARSING\n")
    sb.append("---------------------------\n")
    sb.append(f"- Entrées totales lues      : ${stats.total_movies_parsed}%d\n")
    sb.append(f"- Entrées valides           : ${stats.total_movies_valid}%d\n")
    sb.append(f"- Erreurs de parsing        : ${stats.parsing_errors}%d\n")
    sb.append(f"- Doublons supprimés        : ${stats.duplicates_removed}%d\n\n")

    sb.append("TOP 10 - MEILLEURS FILMS\n")
    sb.append("----------------------------\n")
    report.top_10_rated.zipWithIndex.foreach { case (m, idx) =>
      sb.append(f"${idx + 1}%2d. ${m.title}%-28s : ${m.rating}%.2f/10 (${m.votes}%d votes)\n")
    }
    sb.append("\n")

    sb.append("TOP 10 - PLUS VOTÉS\n")
    sb.append("-----------------------\n")
    report.top_10_by_votes.zipWithIndex.foreach { case (m, idx) =>
      sb.append(f"${idx + 1}%2d. ${m.title}%-28s : ${m.votes}%d votes\n")
    }
    sb.append("\n")

    sb.append("TOP 10 - BOX-OFFICE\n")
    sb.append("-----------------------\n")
    report.highest_grossing.zipWithIndex.foreach { case (m, idx) =>
      sb.append(f"${idx + 1}%2d. ${m.title}%-28s : ${formatMoneyM(m.revenue)}\n")
    }
    sb.append("\n")

    sb.append("TOP 10 - BUDGETS\n")
    sb.append("-------------------\n")
    topBudgets.zipWithIndex.foreach { case ((title, budget), idx) =>
      sb.append(f"${idx + 1}%2d. ${title}%-28s : ${formatMoneyM(budget)}\n")
    }
    sb.append("\n")

    sb.append("RÉPARTITION PAR DÉCENNIE\n")
    sb.append("----------------------------\n")
    report.movies_by_decade.toSeq
      .sortBy { case (label, _) => label.filter(_.isDigit).toIntOption.getOrElse(Int.MaxValue) }
      .foreach { case (decade, count) =>
        sb.append(f"- ${decade}%-25s : ${count}%d films\n")
      }
    sb.append("\n")

    sb.append("RÉPARTITION PAR GENRE\n")
    sb.append("-------------------------\n")
    report.movies_by_genre.toSeq
      .sortBy { case (_, count) => -count }
      .foreach { case (genre, count) =>
        sb.append(f"- ${genre}%-25s : ${count}%d films\n")
      }
    sb.append("\n")

    sb.append("MOYENNES PAR GENRE\n")
    sb.append("----------------------\n")
    sb.append("NOTE MOYENNE :\n")
    report.average_rating_by_genre.toSeq
      .sortBy { case (_, avg) => -avg }
      .foreach { case (genre, avg) =>
        sb.append(f"- ${genre}%-25s : ${avg}%.2f/10\n")
      }

    sb.append("\nDURÉE MOYENNE :\n")
    report.average_runtime_by_genre.toSeq
      .sortBy { case (_, avg) => -avg }
      .foreach { case (genre, avg) =>
        sb.append(f"- ${genre}%-25s : ${avg}%.2f minutes\n")
      }
    sb.append("\n")

    sb.append("TOP 5 - RÉALISATEURS\n")
    sb.append("------------------------\n")
    report.most_prolific_directors.zipWithIndex.foreach { case (d, idx) =>
      sb.append(f"${idx + 1}%2d. ${d.director}%-28s : ${d.count}%d films\n")
    }
    sb.append("\n")

    sb.append("TOP 5 - ACTEURS\n")
    sb.append("-------------------\n")
    report.most_frequent_actors.zipWithIndex.foreach { case (a, idx) =>
      sb.append(f"${idx + 1}%2d. ${a.actor}%-28s : ${a.count}%d films\n")
    }
    sb.append("\n")

    sb.append("RENTABILITÉ\n")
    sb.append("--------------\n")
    sb.append(f"- Films rentables           : ${report.profitable_movies.count}%d films\n")
    sb.append(f"- ROI moyen                 : ${report.profitable_movies.average_roi}%.2fx\n")
    sb.append(bestRoi match {
      case Some(v) => f"- Meilleur ROI              : ${v}%.2fx\n"
      case None => "- Meilleur ROI              : N/A\n"
    })
    sb.append("\n")

    sb.append("PERFORMANCE\n")
    sb.append("---------------\n")
    sb.append(f"- Temps de traitement       : ${durationSeconds}%.3f secondes\n")
    sb.append(f"- Entrées/seconde           : ${entriesPerSecond}%.0f\n\n")

    sb.append("===============================================\n")

    sb.toString
  }

  /**
   * Écrit le rapport texte dans un fichier.
   */
  def writeTextReport(content: String, filename: String): Either[String, Unit] = {
    val writeAttempt = Try {
      val path = Paths.get(filename)
      val parent = Option(path.getParent)
      parent.foreach(p => Files.createDirectories(p))
      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    }

    writeAttempt match {
      case scala.util.Success(_) => Right(())
      case scala.util.Failure(exception) => Left(s"Erreur d'écriture du fichier: ${exception.getMessage}")
    }
  }
}