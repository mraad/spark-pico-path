package com.esri

/**
  * A track.
  *
  * @param trackID the track identifier.
  * @param track   sequence of targets.
  */
case class Track(trackID: Int, track: Array[Target]) {
  /**
    * @return WKT representation of this track.
    */
  def wkt(): String = track.map(_.wkt).mkString("LINESTRING(", ",", ")")
}
