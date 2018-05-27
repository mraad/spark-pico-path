package com.esri

import java.sql.Timestamp

/**
  * Class to represent a cell in space and time.
  *
  * @param q   the cell column
  * @param r   the cell row
  * @param ts  the cell timestamp
  * @param vel the cell avg velocity
  * @param deg the cell avg degree
  */
case class PicoCell(q: Long, r: Long, ts: Timestamp, vel: Double, deg: Double)
