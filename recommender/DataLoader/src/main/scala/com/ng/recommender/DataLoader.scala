package com.ng.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author: Cedaris
  * @Date: 2019/7/2 11:17
  */
object DataLoader {

  val PRODUCT_DATA_PATH = "E:\\MyCode\\MyRecommendSystem\\recommender" +
    "\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "E:\\MyCode\\MyRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"

  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"


  def main(args: Array[String]): Unit = {
    val config: Map[String, String] = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://psy833:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建sparkConf配置
    val conf: SparkConf = new SparkConf().setMaster(config("spark.cores"))
      .setAppName("DataLoader")
    //创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 在对DataFrame和Dataset进行操作许多操作都需要这个包进行支持
    import spark.implicits._

    //将product和rating数据集加载进来
    val productRDD: RDD[String] = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    //将ProdcutRDD装换为DataFrame
    val productDF: DataFrame = productRDD.map(item => {
      val attr: Array[String] = item.split("\\^")
      Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    //将ratingRDD转换为DataFrame
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    //声明一个隐式的MongoDB配置对象
    implicit val mongoConfig = MongoConfig(
      config.get("mongo.uri").get,
      config.get("mongo.db").get)

    //将数据保存到MongoDB中
    storeDataInMongoDB(productDF, ratingDF)

    //关闭spark
    spark.stop()
  }

  //将数据写入mongodb
  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)
                        (implicit mongoConfig: MongoConfig): Unit = {
    //新建一个到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 定义通过MongoDB客户端拿到的表操作对象
    val productCollection: MongoCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection: MongoCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    //如果MongoDB中有对应的数据库，那么应该删除
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //将当前数据写入到MongoDB
    productDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    productCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))

    //关闭MongoDB连接
    mongoClient.close()
  }
}

// 定义样例类
case class Product(productId: Int, name: String, imageUrl: String,
                   categories: String, tags: String)

case class Rating(userId: Int, productId: Int, score: Double,
                  timestamp: Int)

case class MongoConfig(uri: String, db: String)
