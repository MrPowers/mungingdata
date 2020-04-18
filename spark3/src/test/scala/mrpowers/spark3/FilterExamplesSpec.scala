package com.github.mrpowers.spark.spec.sql

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import mrpowers.spark3.SparkSessionTestWrapper
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.scalatest.FunSpec
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.Column

class FilterExamplesSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DatasetComparer {

  import spark.implicits._

  it("shows how to filter with where") {
    val df = Seq(
      ("famous amos", true),
      ("oreo", true),
      ("ginger snaps", false)
    ).toDF("cookie_type", "contains_chocolate")

    df.show()

    val filteredDF = df.where(col("contains_chocolate") === lit(true))

    filteredDF.show()

    // alternate syntax 1
    df.where("contains_chocolate = true").show()

    // alternate syntax 2
    df.where($"contains_chocolate" === true).show()

    // alternate syntax 3
    df.where('contains_chocolate === true).show()
  }

  it("demonstrates how empty memory partitions are made after filtering") {
    val path = new java.io.File("./src/test/resources/person_data.csv").getCanonicalPath
    val df = spark.read.option("header", "true").csv(path)
    df
      .repartition(col("person_country"))
      .filter(col("person_country") === "Cuba")
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("tmp/cuba_data")
  }

}

