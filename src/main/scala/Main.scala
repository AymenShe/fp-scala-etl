package ETL

object Main extends App {

  val filename = "data/data_dirty.json"

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
          println(f"Rapport écrit dans output/results.json ($duration%.3f s)")
      }
  }
}