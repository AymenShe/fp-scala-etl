package ETL

import io.circe.parser._
import io.circe.generic.auto._
import scala.io.Source
import scala.util.{Try, Success, Failure}

case class LoadResult(
  movies: List[Movie],
  totalEntries: Int,
  decodingErrors: Int
)

object DataLoader {
  /**
   * Lit un fichier JSON et retourne les films décodables.
   * Ne fail pas si une entrée est invalide: elle est ignorée, mais comptée.
   */
  def loadMovies(filename: String): Either[String, LoadResult] = {
    val file = Try {
      val source = Source.fromFile(filename)
      try source.mkString
      finally source.close()
    }
    file match {
      case Success(content) =>
        parse(content) match {
          case Left(err) => Left(s"Erreur de parsing JSON: ${err.getMessage}")
          case Right(json) =>
            json.asArray match {
              case None => Left("Le fichier JSON ne contient pas un tableau de films")
              case Some(arr) =>
                var decodingErrors = 0
                val movies = arr.flatMap { elem =>
                  elem.as[Movie] match {
                    case Right(m) => Some(m)
                    case Left(_) =>
                      decodingErrors += 1
                      None
                  }
                }.toList

                Right(
                  LoadResult(
                    movies = movies,
                    totalEntries = arr.size,
                    decodingErrors = decodingErrors
                  )
                )
            }
        }
      case Failure(exception) => Left(s"Erreur de lecture du fichier: ${exception.getMessage}")
    }
  }
}