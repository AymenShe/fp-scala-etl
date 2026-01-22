package ETL

object Main extends App {

  println("ETL : Analyse de Films\n")

  val etlResult = for {
      // Charger données dirty pour produire le log d'erreurs détaillé
      detailed <- DataLoader.loadMoviesDetailed("data/data_large.json")
      (validFromDirty, errors) = detailed
      _ = ErrorLogger.writeParsingErrors(errors, "parsing_errors.log")
      _ = println(s"Log écrit: parsing_errors.log (${errors.length} erreurs)")

      // Charger données clean pour génération de rapport
      movies <- DataLoader.loadMovies("data/data_large.json")
      _ = println(s"${movies.length} films chargés")

    // Statistiques et logs demandés
    stats = StatsCalculator.calculateStats(movies)
    _ = {
      println("\nSTATISTIQUES DE PARSING")
      println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
      println(f"Entrées totales lues    : ${stats.total_movies_parsed}%d")
      println(f"Entrées valides         : ${stats.total_movies_valid}%d")
      println(f"Erreurs de parsing      : ${stats.parsing_errors}%d")
      println(f"Doublons supprimés      : ${stats.duplicates_removed}%d")
    }

    report = ReportGenerator.generateReport(movies)
    _ <- ReportGenerator.writeReport(report, "results.json")
    _ = println("Rapport écrit dans results.json")
  } yield report

  etlResult match {
    case Right(_) =>
      println("\nPipeline ETL terminé avec succès !")
    case Left(error) =>
      println(s"Erreur lors du pipeline ETL : $error")
  }
}