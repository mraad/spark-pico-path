import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object PicoPathApp extends App {

  val spark = SparkSession.builder()
      .appName(getClass.getName)
      .getOrCreate()

  val conf = spark.sparkContext.getConf

  spark.conf.set("spark.sql.session.timeZone", "UTC")
  val outputPath = conf.get("spark.app.output.path", "/tmp/cells")
  val cellSize = conf.getDouble("spark.app.cell.size", 0.0005)
  val cellHalf = cellSize / 2.0
  val secMin = conf.getLong("spark.app.min.sec", 1)
  val secMax = conf.getLong("spark.app.max.sec", 3 * 60)
  val distMin = conf.getDouble("spark.app.min.dist", 0.0)
  val distMax = conf.getDouble("spark.app.max.dist", 1000.0)
  val velMin = conf.getDouble("spark.app.min.vel", 1.0)
  val velMax = conf.getDouble("spark.app.max.vel", 55.0)
  val minPop = conf.getInt("spark.app.min.pop", 1)

  val ws = Window.partitionBy("id").orderBy("pt")


  //MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,VesselType,Status,Length,Width,Draft,Cargo

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
      .csv("AIS_ASCII_by_UTM_Month/2017_v2")

  val tmp = input_df
      .select(
        $"MMSI".as("id"),
        $"BaseDateTime".as("pt"),
        $"LON".as("px"),
        $"LAT".as("py"))
      .filter($"Status" === "under way using engine")

  val dLat = ($"ny" - $"py") / 180 * math.Pi
  val dLon = ($"nx" - $"px") / 180 * math.Pi
  val dy = sin(dLat * 0.5)
  val dx = sin(dLon * 0.5)
  val a = dy * dy + dx * dx * cos($"py") * cos($"ny")
  val R2 = 2.0 * 6371008.8 // meters
  val dist = asin(pow(a, 0.5)) * R2

  val qr = tmp
      .withColumn("nt", lead("pt", 1).over(ws))
      .withColumn("nx", lead("px", 1).over(ws))
      .withColumn("ny", lead("py", 1).over(ws))
      .filter("pt < nt")
      .withColumn("prev_time", unix_timestamp($"pt"))
      .withColumn("next_time", unix_timestamp($"nt"))
      .withColumn("sec", $"next_time" - $"prev_time")
      .filter($"sec" > secMin && $"sec" < secMax)
      .withColumn("dist", dist)
      .filter($"dist" > distMin && $"dist" < distMax)
      .withColumn("vel", ($"dist" / $"sec") * 3.6)
      .filter($"vel" > velMin && $"dist" < velMax)
      .withColumn("deg", atan(($"ny" - $"py") / ($"nx" - $"px")) / math.Pi * 180)
      .withColumn("col", (floor(($"nx" + $"px") * 0.5) / cellSize).cast(LongType))
      .withColumn("row", (floor(($"ny" + $"py") * 0.5) / cellSize).cast(LongType))
      .select($"col" as "q", $"row" as "r", $"pt" as "ts", $"vel", $"deg")

  qr.createTempView("qr")
  println(qr.schema)

  val sql =
    s"""
       |select
       | q * cast($cellSize as double) + cast($cellHalf as double) as x
       |,r * cast($cellSize as double) + cast($cellHalf as double) as y
       |,hour(ts) as hh
       |,count(1) as pop
       |,floor(avg(vel)) as vel
       |,floor(avg(deg)) as deg
       | from qr
       | group by q,r,hour(ts)
       | having pop >= $minPop
   """.stripMargin

  val result = spark.sql(sql)
  result.explain(true)
  result
      .write
      .option("delimiter", ",")
      .mode("overwrite")
      .csv("/tmp/window_func_test")
}
