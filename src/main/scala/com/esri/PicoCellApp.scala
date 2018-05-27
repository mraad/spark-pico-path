package com.esri

import com.esri.gdb._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object PicoCellApp extends App {

  val spark = SparkSession.builder()
    .appName(getClass.getName)
    .getOrCreate()
  try {
    import spark.implicits._

    val conf = spark.sparkContext.getConf
    val inputPath = conf.get("spark.app.input.path", "Miami.gdb")
    val inputName = conf.get("spark.app.input.name", "Broadcast")
    val outputPath = conf.get("spark.app.output.path", "/tmp/cells")
    val outputDelimiter = conf.get("spark.app.output.delimiter", ",")
    val cellSize = conf.getDouble("spark.app.cell.size", 0.0005)
    val cellHalf = cellSize / 2.0
    val secMin = conf.getLong("spark.app.min.sec", 0)
    val secMax = conf.getLong("spark.app.max.sec", 3 * 60)
    val distMin = conf.getDouble("spark.app.min.dist", 0.0)
    val distMax = conf.getDouble("spark.app.max.dist", 1000.0)
    val velMin = conf.getDouble("spark.app.min.vel", 1.0)
    val velMax = conf.getDouble("spark.app.max.vel", 55.0)
    val minPop = conf.getInt("spark.app.min.pop", 1)

    spark
      .read
      .gdb(inputPath, inputName)
      .select(
        $"MMSI".as("id"),
        $"BaseDateTime".as("pt"),
        $"Shape.x".as("px"),
        $"Shape.y".as("py"))
      .where("status = 0") // 0 = under way using engine
      .createTempView("tmp")

    val ws = Window.partitionBy("id").orderBy("pt")

    spark
      .sql("select id,pt,px,py from tmp")
      .withColumn("nt", lead("pt", 1).over(ws))
      .withColumn("nx", lead("px", 1).over(ws))
      .withColumn("ny", lead("py", 1).over(ws))
      .filter("pt is not null and nt is not null and pt < nt") // pt < nt is to ensure positive movement.
      .as[PicoPath]
      .flatMap(pp => {
        val prevTime = pp.pt.getTime
        val nextTime = pp.nt.getTime
        val sec = (nextTime - prevTime) / 1000L
        if (secMin < sec && sec < secMax) {
          val dist = Haversine.distance(pp.py, pp.px, pp.ny, pp.nx)
          if (distMin < dist && dist < distMax) {
            val vel = 3.6 * dist / sec
            if (velMin < vel && vel < velMax) {
              val dy = pp.ny - pp.py
              val dx = pp.nx - pp.px
              val deg = math.atan2(dy, dx).toDegrees
              val col = math.floor((pp.px + pp.nx) * 0.5 / cellSize).toLong
              val row = math.floor((pp.py + pp.ny) * 0.5 / cellSize).toLong
              Some(PicoCell(col, row, pp.pt, vel, deg))
            }
            else {
              None
            }
          } else {
            None
          }
        }
        else {
          None
        }
      })
      .createTempView("qr")

    val sql =
      s"""
         |select
         | q * $cellSize + $cellHalf as x
         |,r * $cellSize + $cellHalf as y
         |,hour(ts) as hh
         |,count(1) as pop
         |,round(avg(vel)) as vel
         |,round(avg(deg)) as deg
         | from qr
         | group by q,r,hour(ts)
         | having pop >= $minPop
       """.stripMargin

    spark.sql(sql)
      .write
      .option("delimiter", outputDelimiter)
      .mode(SaveMode.Overwrite)
      .csv(outputPath)

  } finally {
    spark.stop()
  }
}
