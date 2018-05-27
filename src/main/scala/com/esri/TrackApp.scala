package com.esri

import com.esri.gdb._
import org.apache.spark.sql.{SaveMode, SparkSession}

object TrackApp extends App {

  val spark = SparkSession.builder()
    .appName(getClass.getName)
    .getOrCreate()
  try {
    import spark.implicits._

    spark.udf.register("assemble", new TrackAssembler())

    val inputPath = spark.conf.get("spark.app.input.path", "Miami.gdb")
    val inputName = spark.conf.get("spark.app.input.name", "Broadcast")
    val outputPath = spark.conf.get("spark.app.output.path", "/tmp/tracks")
    val outputDelimiter = spark.conf.get("spark.app.output.delimiter", "\t")

    spark.read
      .gdb(inputPath, inputName)
      .createTempView("tab")

    spark.sql(
      """
        |select trackID,assemble(t,x,y) as track from (
        |select VoyageID as trackID,BaseDateTime as t,Shape.x as x,Shape.y as y,date_format(BaseDateTime,'yyyy-MM-dd') as yymmdd
        |from tab
        |where Status=0
        |sort by VoyageID,BaseDateTime
        |) group by trackID,yymmdd
      """.stripMargin)
      .as[Track]
      .map(track => (track.trackID, track.wkt))
      .write
      .option("delimiter", outputDelimiter)
      .mode(SaveMode.Overwrite)
      .csv(outputPath)

  } finally {
    spark.sparkContext.stop()
  }

}
