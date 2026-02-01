package ETL

object Main extends App {

  val filename = "data/data_large.json"

  val start = System.currentTimeMillis()

  DataLoader.loadMovies(filename) match {
    case Left(err) =>
      println(err)

    case Right(load) =>
      val report = ReportGenerator.generateReport(load)
      ReportGenerator.writeReport(report, "output/results.json") match {
        case Left(writeErr) =>
          println(writeErr)
        case Right(_) =>
          val duration = (System.currentTimeMillis() - start) / 1000.0
          val textReport = ReportGenerator.generateTextReport(load, report, duration)
          ReportGenerator.writeTextReport(textReport, "output/report.txt") match {
            case Left(txtErr) =>
              println(txtErr)
            case Right(_) =>
              println("Rapports écrits dans output/results.json et output/report.txt")
          }
          println(f"Durée d'exécution : ($duration%.3f s)")
      }
  }
}