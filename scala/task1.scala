import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.io.{PrintWriter, File}
import scala.io.Source


object task1 {
  def jsonify(review: JValue):(String, Int) = {
    implicit val formats = DefaultFormats
    return ((review \ "user_id").extract[String], 1)
  }

  def BHelper(review: JValue, year: String):Boolean = {
    implicit val formats = DefaultFormats
    val str: String = (review \ "date").extract[String]
    val pyear = str.substring(0, 4)
    return pyear == year
  }

  def wrangle(review: JValue): String = {
    val punctuations = Set('(', '[', ',', '.', '!', '?', ':', ';', ']', ')')

    var ret = new StringBuilder()
    implicit val formats = DefaultFormats

    for (chr: Char <- review.extract[String]) {
      if (punctuations.contains(chr)) {
        ret.append(' ')
      }
      else if (chr.isLetter || chr == ' ') {
        ret.append(chr)
      }
      else {
        ret.append(' ')
      }
    }

    return ret.toString().toLowerCase()
  }

  def A(data: org.apache.spark.rdd.RDD[JValue]): Long = {
    return data.count()
  }

  def B(data: org.apache.spark.rdd.RDD[JValue], year: String): Long = {
    return data.filter(x => BHelper(x, year)).count()
  }

  def C(data: org.apache.spark.rdd.RDD[(String, Int)]): Long = {
    return data.distinct().count()
  }

  def D(data: org.apache.spark.rdd.RDD[(String, Int)], top_m: Int): Array[List[Any]] = {
    return data.reduceByKey(_+_).sortBy(x => (-x._2, x._1)).take(top_m).map(item => item.productIterator.toList)
  }

  def E(data: RDD[String], stopwords: Set[String], top_n: Int): Array[String] = {
    return data.flatMap(review => review.split(" +"))
      .filter(word => !stopwords.contains(word))
      .map(word => (word, 1))
      .reduceByKey(_+_)
      .sortBy(x => -x._2)
      .take(top_n).map(_._1)
  }

  def main(args:Array[String]): Unit = {
    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")

    for (arg <- args) {
      println(arg)
    }

    val input_file = args(0)
    val output_file = args(1)
    val stopwords_file = args(2)
    val year = args(3)
    val top_m = args(4).toInt
    val top_n = args(5).toInt

    var output: Map[String, Any] = Map()

    val textRDD = sc.textFile(input_file)
    val data = textRDD.map(review => parse(review))
    data.cache()

    val reviewers = data.map(review => jsonify(review))
    reviewers.cache()

    val reviews = data.map(review => review \ "text")
      .map(line => wrangle(line))

    val stopwords = Source.fromFile(stopwords_file).getLines.toSet

    val a = A(data)
    val b = B(data, year)
    val c = C(reviewers)
    val d = D(reviewers, top_m)
    val e = E(reviews, stopwords, top_n)

    output += {"A"->a}
    output += {"B"->b}
    output += {"C"->c}
    output += {"D"->d}
    output += {"E"->e}

    implicit val formats = org.json4s.DefaultFormats
    val formatted_output = org.json4s.jackson.Serialization.write(output)

    println(formatted_output)

    val out = new PrintWriter(new File(output_file))
    out.write(formatted_output)
    out.close()
  }
}