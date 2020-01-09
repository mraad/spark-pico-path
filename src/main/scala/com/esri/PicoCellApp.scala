package com.esri

import com.esri.gdb._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object PicoCellApp extends App {

  val spark = SparkSession.builder()
    .appName(getClass.getName)
    .getOrCreate()
  try {
    import spark.implicits._

    val conf = spark.sparkContext.getConf
    conf.set("spark.sql.session.timeZone", "UTC")
    val inputFormat: String = conf.get("spark.app.input.format", "gdb")

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

    val tmp = inputFormat.toLowerCase match {
      case "ais_csv" =>
        val schema = new StructType()
            .add("MMSI", IntegerType)
            .add("BaseDateTime", TimestampType)
            .add("LAT", FloatType)
            .add("LON", FloatType)

            .add("SOG", StringType)
            .add("COG", StringType)
            .add("Heading", StringType)
            .add("VesselName", StringType)
            .add("IMO", StringType)
            .add("CallSign", StringType)
            .add("VesselType", StringType)

            .add("Status", StringType)

            .add("Length", StringType)
            .add("Width", StringType)
            .add("Draft", StringType)
            .add("Cargo", StringType)
        val input_df = spark.read
            .schema(schema)
            .option("header", "true")
            .csv(inputPath)
        input_df
            .select(
              $"MMSI".as("id"),
              $"BaseDateTime".as("pt"),
              $"LON".as("px"),
              $"LAT".as("py"))
            .filter($"Status" === "under way using engine")
      case "gdb" =>
        spark
            .read
            .gdb(inputPath, inputName)
            .select(
              $"MMSI".as("id"),
              $"BaseDateTime".as("pt"),
              $"Shape.x".as("px"),
              $"Shape.y".as("py"))
            .where("status = 0") // 0 = under way using engine
      case unsupported =>
        Console.err.println(s"unsupported input format: $unsupported, only 'ais_csv' and 'gdb' are available ")
        sys.exit(1)
    }

    val ws = Window.partitionBy("id").orderBy("pt")

    val dLat = ($"ny" - $"py") / 180 * math.Pi
    val dLon = ($"nx" - $"px") / 180 * math.Pi
    val dy = sin(dLat * 0.5)
    val dx = sin(dLon * 0.5)
    val a = dy * dy + dx * dx * cos($"py" / 180 * math.Pi) * cos($"ny" / 180 * math.Pi)
    val R2 = 2.0 * 6371008.8 // meters
    val dist = asin(sqrt(a)) * R2

    tmp
        .withColumn("nt", lead("pt", 1).over(ws))
        .withColumn("nx", lead("px", 1).over(ws))
        .withColumn("ny", lead("py", 1).over(ws))
        .withColumn("prev_time", unix_timestamp($"pt"))
        .withColumn("next_time", unix_timestamp($"nt"))
        .withColumn("sec", $"next_time" - $"prev_time")
        .filter($"sec" > secMin && $"sec" < secMax)
        .withColumn("dist", dist)
        .filter($"dist" > distMin && $"dist" < distMax)
        .withColumn("vel", ($"dist" / $"sec") * 3.6)
        .filter($"vel" > velMin && $"vel" < velMax)
        .withColumn("deg", atan2($"ny" - $"py", $"nx" - $"px") / math.Pi * 180)
        .withColumn("col", floor(($"nx" + $"px") * 0.5 / cellSize).cast(LongType))
        .withColumn("row", floor(($"ny" + $"py") * 0.5 / cellSize).cast(LongType))
        .select($"col" as "q", $"row" as "r", $"pt" as "ts", $"vel", $"deg")
        .createTempView("qr")

    val sql =
      s"""
         |select
         |q * ${cellSize}d + ${cellHalf}d as x
         |,r * ${cellSize}d + ${cellHalf}d as y
         |,hour(ts) as hh
         |,count(1) as pop
         |,floor(avg(vel)) as vel
         |,floor(avg(deg)) as deg
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
