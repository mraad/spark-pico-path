package com.esri

import java.sql.Timestamp

/**
  * A pico path.
  *
  * @param id the path track identifier.
  * @param pt the previous timestamp.
  * @param px the previous horizontal location.
  * @param py the previous vertical location.
  * @param nt the next timestamp.
  * @param nx the next horizontal location.
  * @param ny the next vertical location.
  */
case class PicoPath(id: Int,
                    pt: Timestamp, px: Double, py: Double,
                    nt: Timestamp, nx: Double, ny: Double
                   ) {
  /**
    * @return WKT representation of the path.
    */
  def wkt(): String = {
    f"LINESTRING($px%.6f $py%.6f,$nx%.6f $ny%.6f)"
  }
}
