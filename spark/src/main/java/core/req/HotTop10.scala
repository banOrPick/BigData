package core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HotTop10 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TOP10")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("datas/user_visit_action.txt")
    val clickCountRDD = rdd.filter(line => {
      val strings = line.split("_")
      strings(6) != "-1"
    }).map(line => {
      val strings = line.split("_")
      (strings(6), 1)
    }).reduceByKey(_ + _)

    val orderCountRDD = rdd.filter(action => {
      val datas = action.split("_")
      datas(8) != "null"
    }).flatMap(line => {
      val strings = line.split("_")
      var oIds = strings(8)
      val strings1 = oIds.split(",")
      strings1.map(id => (id, 1))
    }).reduceByKey(_ + _)

    val payCountRDD = rdd.filter(action => {
      val datas = action.split("_")
      datas(10) != "null"
    }).flatMap(line => {
      val strings = line.split("_")
      var oIds = strings(10)
      val strings1 = oIds.split(",")
      strings1.map(id => (id, 1))
    }).reduceByKey(_ + _)

    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
      clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD = cogroupRDD.mapValues{
      case ( clickIter, orderIter, payIter ) => {

        var clickCnt = 0
        val iter1 = clickIter.iterator
        if ( iter1.hasNext ) {
          clickCnt = iter1.next()
        }
        var orderCnt = 0
        val iter2 = orderIter.iterator
        if ( iter2.hasNext ) {
          orderCnt = iter2.next()
        }
        var payCnt = 0
        val iter3 = payIter.iterator
        if ( iter3.hasNext ) {
          payCnt = iter3.next()
        }

        ( clickCnt, orderCnt, payCnt )
      }
    }

    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

    // 6. 将结果采集到控制台打印出来
    resultRDD.foreach(println)
    sc.stop()
  }

}
