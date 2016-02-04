package main.scala.com.github.shagunsodhani.utils

import java.net.InetAddress
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.typesafe.config.ConfigFactory

object SparkContextUtils {

  val config = ConfigFactory.load();
  
  private val appName = config.getString("spark.appname")

  private val master = config.getString("spark.master")

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