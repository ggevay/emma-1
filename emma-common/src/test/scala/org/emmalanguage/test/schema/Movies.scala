package org.emmalanguage.test.schema

object Movies {
  case class ImdbMovie(title: String, rating: Double, rank: Int, link: String, year: Int)
  case class ImdbYear(year: Int)
  case class FilmFestWinner(year: Int, title: String, director: String, country: String)
}
