package com.esri

import com.esri.gdb._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object PicoPathApp extends App {

  val spark = SparkSession.builder()
    .appName(getClass.getName)
    .getOrCreate()
  try {
    import spark.implicits._

    val conf = spark.sparkContext.getConf
    val inputPath = conf.get("spark.app.input.path", "Miami.gdb")
    val inputName = conf.get("spark.app.input.name", "Broadcast")
    val outputPath = conf.get("spark.app.output.path", "/tmp/paths")
    val outputDelimiter = conf.get("spark.app.output.delimiter", "\t")

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
      .filter("pt is not null and nt is not null and pt < nt") // Ensure non-zero and positive Δτ.
      .as[PicoPath]
      .map(pp => {
        val prevTime = pp.pt.getTime
        val nextTime = pp.nt.getTime
        // PicoPath time
        val seconds = (nextTime - prevTime) / 1000L
        // PicoPath length
        val meters = Haversine.distance(pp.py, pp.px, pp.ny, pp.nx)
        // PicoPath speed in kilometers per hour
        val kph = 3.6 * meters / seconds
        val dy = pp.ny - pp.py
        val dx = pp.nx - pp.px
        // PicoPath arithmetic direction of travel.
        val deg = math.atan2(dy, dx).toDegrees
        (pp.id, meters, seconds, kph, deg, pp.wkt)
      }
      )
      .write
      .option("delimiter", outputDelimiter)
      .mode(SaveMode.Overwrite)
      .csv(outputPath)
  }
  finally {
    spark.stop()
  }

}
