
package ETL

object StatsCalculator {

  /**
   * Calcule les statistiques générales
   */
  def calculateStats(movies: List[Movie]): MovieStats = {
    // TODO: Calculer :
    //   - total : taille de la liste
    //   - avgRating : somme des ratings / nombre de restaurants
    //   - vegCount : compter ceux avec vegetarianOptions = true
    // ATTENTION : gérer le cas liste vide pour éviter division par 0 !
    if (restaurants.isEmpty) {
      RestaurantStats(0, 0.0, 0)
    } else {
      val total = restaurants.length
      val avgRating = restaurants.map(_.rating).sum / total
      val vegCount = restaurants.count(_.vegetarianOptions)
      RestaurantStats(total, avgRating, vegCount)
    }
  }

  /**
   * Top N restaurants par note
   */
  def topRated(restaurants: List[Restaurant], n: Int = 3): List[TopRestaurant] = {
    // TODO: 
    //   1. Trier par rating décroissant (utiliser sortBy avec -)
    //   2. Prendre les n premiers (take)
    //   3. Mapper vers TopRestaurant
    restaurants
      .sortBy(- _.rating)
      .take(n)
      .map(r => TopRestaurant(r.name, r.rating))
  }

  /**
   * Compte par type de cuisine
   */
  def countByCuisine(restaurants: List[Restaurant]): Map[String, Int] = {
    // TODO: 
    //   1. Grouper par cuisine (groupBy)
    //   2. Compter la taille de chaque groupe (map)
    restaurants
      .groupBy(_.cuisine)
      .map { case (cuisine, group) => (cuisine, group.length) }
  }

  /**
   * Compte par gamme de prix
   */
  def countByPriceRange(restaurants: List[Restaurant]): Map[String, Int] = {
    // TODO: Comme countByCuisine mais grouper par priceRange
    // ATTENTION : convertir priceRange en String pour la Map
    restaurants
      .groupBy(r => r.priceRange.toString)
      .map { case (priceRange, group) => (priceRange, group.length) }
  }
}