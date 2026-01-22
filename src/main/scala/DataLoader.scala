package miniEtl

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import scala.io.Source
import scala.util.{Try, Success, Failure}

object DataLoader {

  /**
   * Lit un fichier JSON et parse les restaurants
   */
  def loadMovies(filename: String): Either[String, List[Restaurant]] = {
    // TODO: Utiliser Try pour lire le fichier
    //   1. Créer un Source.fromFile(filename)
    //   2. Lire le contenu avec source.mkString
    //   3. Fermer le fichier avec source.close() - IMPORTANT !
    //   4. Parser avec decode[List[Restaurant]](content)
    //   5. Gérer les erreurs avec pattern matching
    val file = Try {
        val source = Source.fromFile(filename)
        try source.mkString
        finally source.close()
    }
    file match {
      case Success(content) => decode[List[Restaurant]](content) match {
        case Right(restaurants) => Right(restaurants)
        case Left(error) => Left(s"Erreur de parsing JSON: ${error.getMessage}")
      }
      case Failure(exception) => Left(s"Erreur de lecture du fichier: ${exception.getMessage}")
    }
  }
}