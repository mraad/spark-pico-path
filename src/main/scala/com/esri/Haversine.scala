package com.esri

import scala.math._

/**
  * https://rosettacode.org/wiki/Haversine_formula#Scala
  * https://en.wikipedia.org/wiki/Earth_radius
  */
object Haversine extends Serializable {

  val R2 = 2.0 * 6371008.8 // meters

  def distance(lat1: Double, lon1: Double, lat2: Double, lon2: Double) = {

    val dLat = (lat2 - lat1).toRadians
    val dLon = (lon2 - lon1).toRadians

    val dy = sin(dLat * 0.5)
    val dx = sin(dLon * 0.5)

    val a = dy * dy + dx * dx * cos(lat1.toRadians) * cos(lat2.toRadians)

    R2 * asin(sqrt(a))
  }
}
