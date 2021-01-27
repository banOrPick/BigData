package core.framework.common

import com.banorpick.bigdata.spark.core.framework.util.EnvUtil

trait TDao {

  def readFile(path: String) = {
    EnvUtil.take().textFile(path)
  }
}
