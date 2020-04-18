package com.github.mrpowers.spark.spec.sql

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import mrpowers.spark3.SparkSessionTestWrapper
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.scalatest.FunSpec
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.Column

class NewArrayFunctionsSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DatasetComparer {

  describe("exists") {
    it("displays a fake dataset") {
      val df = spark.createDF(
        List(
          ("a", Array(3, 4, 5)),
          ("b", Array(8, 12)),
          ("c", Array(7, 13)),
          ("d", null),
        ), List(
          ("person_id", StringType, true),
          ("best_numbers", ArrayType(IntegerType, true), true)
        )
      )

      df.show()
    }

    it("returns true if the Array contains an even value") {

      def isEven(col: Column): Column = {
        col % 2 === lit(0)
      }

      val df = spark.createDF(
        List(
          ("a", Array(3, 4, 5)),
          ("b", Array(8, 12)),
          ("c", Array(7, 13)),
          ("d", null),
        ), List(
          ("person_id", StringType, true),
          ("best_numbers", ArrayType(IntegerType, true), true)
        )
      )

      val resDF = df.withColumn(
        "even_best_number_exists",
        exists(col("best_numbers"), isEven)
      )

      resDF.show()

      df.withColumn(
        "even_best_number_exists",
        exists(col("best_numbers"), (col: Column) => col % 2 === lit(0))
      )
.show()

    }

    it("also works with anonymous functions") {

      val df = spark.createDF(
        List(
          ("a", Array(3, 4, 5)),
          ("b", Array(8, 12)),
          ("c", Array(7, 13)),
          ("d", null),
        ), List(
          ("person_id", StringType, true),
          ("best_numbers", ArrayType(IntegerType, true), true)
        )
      )

      df.withColumn(
        "even_best_number_exists",
        exists(col("best_numbers"), (col: Column) => col % 2 === lit(0))
      )

    }
  }

  describe("forall") {
    it("returns true if all the words start with the same letter") {
      val df = spark.createDF(
        List(
          (Array("ants", "are", "animals")),
          (Array("italy", "is", "interesting")),
          (Array("brazilians", "love", "soccer")),
          (null),
        ), List(
          ("words", ArrayType(StringType, true), true)
        )
      )

      val resDF = df.withColumn(
        "uses_alliteration_with_a",
        forall(
          col("words"),
          (col: Column) => col.startsWith("a")
        )
      )

      resDF.show(false)


      //      def usesAlliteration(col: Column): Column = {
//        col.startsWith(col("words")(0).substr(0, 1))
//      }
//
//      val resDF = df.withColumn(
//          "words_first_letter",
//          col("words")(0).substr(0, 1)
//        )
//        .withColumn(
//          "words_uses_alliteration",
//          forall(
//            col("words"),
//            usesAlliteration
//          )
//        )
//
//      resDF.show()

    }
  }

  describe("filter") {
    it("filters out the undesirable words") {
//      val df = spark.createDF(
//        List(
//          (Array("bad", "dirty"), Array("bad", "bunny", "is", "funny")),
//          (Array("mal", "suicio"), Array("eres", "suicio", "mi", "mal", "amigo")),
//          (null),
//        ), List(
//          ("blacklisted", ArrayType(StringType, true), true),
//          ("words", ArrayType(StringType, true), true)
//        )
//      )
//
//      def cleanWord(blacklisted: Column)(word: Column): Column = {
//        array_contains(blacklisted, word)
//      }
//
//      val resDF = df.withColumn(
//        "clean_words",
//        filter(
//          col("words"),
////          cleanWord(col("blacklisted"))
//          (word: Column, blacklisted: Column) => array_contains(col("blacklisted"), word)
//        )
//      )
//
//      resDF.show(false)


      val df = spark.createDF(
        List(
          (Array("bad", "bunny", "is", "funny")),
          (Array("food", "is", "bad", "tasty")),
          (null),
        ), List(
          ("words", ArrayType(StringType, true), true)
        )
      )

      val resDF = df.withColumn(
        "filtered_words",
        filter(
          col("words"),
          (col: Column) => col =!= lit("bad")
        )
      )

      resDF.show(false)
    }

    it("shows how to use filter with two arguments") {

//      val df = spark.createDF(
//        List(
//          ("crazy", Array("she's", "crazy", "cindy")),
//          ("smelly", Array("he's", "smelly", "sam")),
//          (null),
//        ), List(
//          ("blacklisted_word", StringType, true),
//          ("words", ArrayType(StringType, true), true)
//        )
//      )
//
//      df.show(false)
//
//      def isBlacklisted(blacklisted: Column)(col: Column): Column = {
//        col =!= blacklisted
//      }
//
//      val resDF = df.withColumn(
//        "rephrased",
//        filter(
//          col("words"),
////          isBlacklisted(col("blacklisted_word")) _
//          (col: Column) => col =!= col("blacklisted")
//        )
//      )
//
//      resDF.show(false)

    }

  }

  describe("transform") {
    it("maps over an array") {

      val df = spark.createDF(
        List(
          (Array("New York", "Seattle")),
          (Array("Barcelona", "Bangalore")),
          (null),
        ), List(
          ("places", ArrayType(StringType, true), true)
        )
      )

      val resDF = df.withColumn(
        "fun_places",
        transform(
          col("places"),
          (col: Column) => concat(col, lit(" is fun!"))
        )
      )

      resDF.show(false)
    }
  }

  describe("aggregate") {
    it("sums the numbers in an array") {

      val df = spark.createDF(
        List(
          (Array(1, 2, 3, 4)),
          (Array(5, 6, 7)),
          (null),
        ), List(
          ("numbers", ArrayType(IntegerType, true), true)
        )
      )

      val resDF = df.withColumn(
        "numbers_sum",
        aggregate(
          col("numbers"),
          lit(0),
          (col1: Column, col2: Column) => col1 + col2
        )
      )

      resDF.show()
    }
  }

  describe("zip_with") {
    it("combines two arrays") {
      val df = spark.createDF(
        List(
          (Array("a", "b"), Array("c", "d")),
          (Array("x", "y"), Array("p", "o")),
          (null, Array("e", "r"))
        ), List(
          ("letters1", ArrayType(StringType, true), true),
          ("letters2", ArrayType(StringType, true), true)
        )
      )

      df.show()

      val resDF = df.withColumn(
        "zipped_letters",
        zip_with(
          col("letters1"),
          col("letters2"),
          (left: Column, right: Column) => concat_ws("***", left, right)
        )
      )

      resDF.show()

    }
  }

}

