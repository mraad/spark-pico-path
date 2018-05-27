package com.esri

import java.sql.Timestamp

/**
  * A target in a track.
  *
  * @param t the target timestamp.
  * @param x the target horizontal location.
  * @param y the target vertical location.
  */
case class Target(t: Timestamp, x: Double, y: Double) {
  /**
    * @return WKT representation of this target.
    */
  def wkt(): String = f"$x%.6f $y%.6f"
}
