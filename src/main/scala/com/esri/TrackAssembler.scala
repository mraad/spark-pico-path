package com.esri

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  * https://docs.databricks.com/spark/latest/spark-sql/udaf-scala.html
  */
class TrackAssembler extends UserDefinedAggregateFunction {

  // The input schema
  override def inputSchema: StructType = StructType(Seq(
    StructField("t", TimestampType),
    StructField("x", DoubleType),
    StructField("y", DoubleType)
  ))

  // The intermediate schema
  override def bufferSchema: StructType = StructType(Seq(
    StructField("track", ArrayType(
      StructType(Seq(
        StructField("t", TimestampType),
        StructField("x", DoubleType),
        StructField("y", DoubleType)
      ))))
  ))

  // The output schema - Note the Seq !
  override def dataType: DataType = ArrayType(
    StructType(Seq(
      StructField("t", TimestampType),
      StructField("x", DoubleType),
      StructField("y", DoubleType)
    )))

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = mutable.WrappedArray.empty[Row]
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val wa = buffer.getAs[mutable.WrappedArray[Row]](0)
    buffer(0) = wa :+ input
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val l = buffer1.getAs[mutable.WrappedArray[Row]](0)
    val r = buffer2.getAs[mutable.WrappedArray[Row]](0)
    buffer1(0) = l ++ r
  }

  override def evaluate(buffer: Row): Any = {
    // TODO - Test if performing sort here is faster !
    buffer.get(0)
  }
}
