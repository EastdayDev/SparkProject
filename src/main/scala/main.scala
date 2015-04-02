package com.eastday.ml

import org.apache.spark._
import SparkContext._
import org.apache.spark.mllib.recommendation._
import scala.collection.mutable.Map

case class UserChannelAccesses(userId: Int, channelId: Int, count: Int)

object Recommender {

  val userKeyValue: Map[String, Int] = Map()

  def main(args: Array[String]) {
    //testUserChannelToFile("/home/hb/logs/eastday.com.full_webdig_201412030000.log")
    startRecommender("2081275969", "/home/hb/works/logs/")
  }

  def startRecommender(originUser: String, logPath: String){
    val sc = new SparkContext("local", "EastdayRecommander")
    val rawData = sc.textFile(logPath);

    //将用户编号映射为整数 Rating 需要
    val userChannels = rawData.map(parse).reduceByKey(_ + _)

    val trainData = userChannels.map(row=>{
      val pieces = row._1.split("=>")
      val Array(userId, channelId, count) = Array(pieces(0).toInt,pieces(1).toInt, row._2)
      Rating(userId.toInt, channelId.toInt, count)
     }).cache()

     val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
     val iUserId = userKeyValue(originUser);
     val recommendations = model.recommendProducts(iUserId , 5)
     recommendations.foreach(item => {
       println(item.toString().replaceAll(iUserId.toString, originUser))
     })
  }

  def printRating(rating: String)  {
    println(rating)
  }

  def testUserChannelToFile(logFile: String) {
    val sc = new SparkContext("local", "EastdayRecommander")
    val rawData = sc.textFile(logFile);
    val userChannels = rawData.map(originParse).reduceByKey(_ + _)
    val userChannelPV = userChannels.map(row => {
        val pieces = row._1.split("=>")
        (pieces(0), pieces(1), row._2)
      })

    userChannelPV.saveAsTextFile("/home/hb/works/resultFiles/eastday.com.full_webdig_201412030000.rst")
  }

def parse(line: String) = {
    val pieces = line.split("\\s+")
    val userId = pieces(1)
    val channelId = getChannel(pieces(5))
    userKeyValue += (userId->(userKeyValue.size + 10000000))
    (userKeyValue(userId) + "=>" + channelId, 1)
  }

  def originParse(line: String) = {
    val pieces = line.split("\\s+")
    val userId = pieces(1)
    val channelId = getChannel(pieces(5))
    (userId + "=>" + channelId, 1)
  }

  def getChannel(url: String): Int = {
    url match {
      case _ if url.startsWith("\"http://china.eastday.com/c/") => 1
      case _ if url.startsWith("\"http://world.eastday.com/w/") => 2
      case _ if url.startsWith("\"http://society.eastday.com/s/") => 3
      case _ if url.startsWith("\"http://finance.eastday.com/m/") => 4
      case _ if url.startsWith("\"http://mil.eastday.com/m/") => 5
      case _ if url.startsWith("\"http://enjoy.eastday.com/e/") => 6
      case _ if url.startsWith("\"http://sh.eastday.com/m/") => 7
      case _ if url.startsWith("\"http://sports.eastday.com/s/") => 8
      case _ if url.startsWith("\"http://history.eastday.com/h/") => 9
      case _ if url.startsWith("\"http://msnphoto.eastday.com/2013slideshow/") => 10
      case _ if url.startsWith("\"http://imedia.eastday.com/node2/2013imedia/i/") => 11
      case _ if url.startsWith("\"http://pinglun.eastday.com/p/") => 12
      case _ if url.startsWith("\"http://english.eastday.com/e/") => 13
      case _ if url.startsWith("\"http://ej.eastday.com") => 14
      case _ if url.startsWith("\"http://news.eastday.com") => 15
      case _ => 0
    }
  }
}
