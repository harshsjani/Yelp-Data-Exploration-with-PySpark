import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.io.{File, PrintWriter}
import scala.io.Source
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.BigDecimal


object task2 {
  implicit val formats = DefaultFormats

  def format_rdata(row: String): Tuple2[String, Float] = {
    val data = parse(row)
    return ((data \ "business_id").extract[String], (data \ "stars").extract[Float])
  }

  def format_bdata(row: String): Tuple2[String, String] = {
    val data = parse(row)
    return ((data \ "business_id").extract[String], (data \ "categories").extract[String])
  }

  def split_categories(row: Tuple2[String, String]): List[Tuple2[String, String]] = {
    var out = new ListBuffer[Tuple2[String, String]]()
    for (x <- row._2.trim().split(",")) {
      out += ((row._1, x.trim()))
    }
    return out.toList
  }

  def do_spark(review_file: String, business_file: String, top_n: Int): Array[List[Any]] = {
    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")

    val reviewRDD = sc.textFile(review_file).map(row => format_rdata(row))
    val bizRDD = sc.textFile(business_file).map(row => format_bdata(row))
      .filter(row => row._2 != null)
      .flatMap(row => split_categories(row))

    val zero_value = (0.0, 0.0)
    val seq_fn = (x: Tuple2[Double, Double], y: Float) => ((x._1 + y).toDouble, x._2 + 1.0)
    val comb_fn = (x: Tuple2[Double, Double], y: Tuple2[Double, Double]) => (x._1 + y._1, x._2 + y._2)

    return reviewRDD.join(bizRDD)
      .map(x => (x._2._2, x._2._1))
      .aggregateByKey(zero_value)(seq_fn, comb_fn)
      .mapValues(x => BigDecimal(x._1 / x._2).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble)
      .sortBy(x => (-x._2, x._1))
      .take(top_n).map(item => item.productIterator.toList)
  }

  def do_py(review_file: String, business_file: String, top_n: Int): Array[List[Any]] = {
    val review_lines = Source.fromFile(review_file).getLines.map(format_rdata)
    val biz_lines = Source.fromFile(business_file).getLines
      .map(format_bdata)
      .filter(row => row._2 != null)
      .flatMap(row => split_categories(row)).toList

    val rdata = review_lines.toSeq.groupBy(_._1).mapValues(_.map(_._2)).toList
    var rdatadict = scala.collection.mutable.Map[String, Seq[Float]]()

    for (x <- rdata) {
      rdatadict(x._1) = x._2
    }

    var cat_sum = scala.collection.mutable.Map[String, Float]()
    var cat_count = scala.collection.mutable.Map[String, Int]()

    for (row <- biz_lines) {
      val b_id = row._1

      if (rdatadict.contains(b_id)) {
        for (x: Float <- rdatadict(b_id)) {
          if (cat_sum.contains(row._2)) {
            cat_sum(row._2) = cat_sum(row._2) + x
          } else {
            cat_sum(row._2) = x
          }

          if (cat_count.contains(row._2)) {
            cat_count(row._2) = cat_count(row._2) + 1
          } else {
            cat_count(row._2) = 1
          }
        }
      }
    }

    var avg_data = new ArrayBuffer[Tuple2[String, Float]]

    for (k <- cat_sum.keys) {
      avg_data += ((k, cat_sum(k) / cat_count(k).toFloat))
    }

    val output_array = avg_data.map(x => (x._1,
      BigDecimal(x._2.toDouble).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
      ))
      .sortBy(x => (-x._2, x._1)).
      toArray
    return output_array.take(top_n).map(item => item.productIterator.toList)
  }

  def main(args:Array[String]): Unit = {
    for (arg <- args) {
      println(arg)
    }

    val review_file = args(0)
    val business_file = args(1)
    val output_file = args(2)
    val use_spark = if (args(3) == "spark") true else false
    val top_n = args(4).toInt

    val output: Map[String, Any]
      = if (use_spark) Map("result"->do_spark(review_file, business_file, top_n))
        else Map("result"->do_py(review_file, business_file, top_n))

    implicit val formats = org.json4s.DefaultFormats
    val formatted_output = org.json4s.jackson.Serialization.write(output)

    println(formatted_output)

    val out = new PrintWriter(new File(output_file))
    out.write(formatted_output)
    out.close()
  }
}