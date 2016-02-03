package main.scala.com.github.shagunsodhani.utils

import java.net.InetAddress
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkContextUtils {
  private val appName = "MCE"

  private val master = "local[1]"

  private val driver = InetAddress.getLocalHost().getHostAddress();

  private val conf = new SparkConf()
    .setAppName(appName)
    .setMaster(master)
    .set("spark.driver.host", driver);

  private val sparkContext = new SparkContext(conf);

  def getSparkContext: SparkContext = {
    sparkContext
  }
}