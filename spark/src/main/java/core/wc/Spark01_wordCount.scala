package core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_wordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wc")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("datas")
    val words = lines.flatMap(line => (line.split(" ")))
    val result = words.map(word => (word, 1)).reduceByKey((x, y) => (x + y))
    result.collect().foreach(println(_))
    sc.stop()
  }

}
