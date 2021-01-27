package core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 案例实操

    // 1. 获取原始数据：时间戳，省份，城市，用户，广告
    val dataRDD = sc.textFile("datas/agent.log")

    dataRDD.map(f = line => {
      val data = line.split(" ")
      ((data(1), data(4)), 1)
    }).reduceByKey(_ + _).map {
      case ((prv, ad), sum) =>
        (prv, (ad, sum))

    }.groupByKey().mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    }).collect().foreach(println)

    sc.stop()
  }
}
