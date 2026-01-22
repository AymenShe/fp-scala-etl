package ETL

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object ErrorLogger {
  def writeParsingErrors(errors: List[ParsingError], filename: String): Either[String, Unit] = {
    val header = "PARSED ERRORS LOG\n=================\n"
    val body = errors.map { e =>
      val msgs = e.messages.mkString("; ")
      s"[index=${e.index}] title='${e.titleHint}' :: ${msgs}"
    }.mkString("\n")
    val content = header + body + "\n"
    try {
      Files.write(Paths.get(filename), content.getBytes(StandardCharsets.UTF_8))
      Right(())
    } catch {
      case ex: Throwable => Left(s"Erreur d'écriture du log: ${ex.getMessage}")
    }
  }
}
