package com.ng.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author: Cedaris
  * @Date: 2019/7/2 11:52
  */
object StatisticsRecommender {
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {
    //配置信息
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://psy833:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._

    //数据加载进来
    val ratingDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    //创建一张名叫ratings的表
    ratingDF.createOrReplaceTempView("ratings")

    //TODO：不同的统计推荐结果
    //1、历史热门商品统计
    //根据所有历史评分数据，计算历史评分次数最多的商品
    //通过Spark SQL读取评分数据集，统计所有评分中评分数最多的商品，然后按照从大到小排序，将最终结果写入MongoDB的RateMoreProducts数据集中
    //数据结构 -》  productId,count
    val rateMoreProductsDF: DataFrame = spark.sql("select productId,count(productId) as count from ratings group " +
      "by productId")
    rateMoreProductsDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MORE_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //2、最近热门商品统计最近热门商品统计
    /*
    根据评分，按月为单位计算最近时间的月份里面评分数最多的商品集合。
     实现思路：
     通过Spark SQL读取评分数据集，通过UDF函数将评分的数据时间修改为月，然后统计每月商品的评分数。
     统计完成之后将数据写入到MongoDB的RateMoreRecentlyProducts数据集中
     */
    //统计以月为单位拟每个商品的评分数
    //数据结构 -》 productId,count,time
    //创建一个时间格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    //注册一个UDF函数，用于将timestamp转换成年月格式
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new
        Date(x * 1000L)).toInt)

    //将原来的Rating数据集中的时间转换成年月的格式
    val ratingOfYearMonth: DataFrame = spark.sql("select productId,score,changeDate(timestamp) as yearmonth from" +
      " ratings")
    //将新的数据集注册为新的临时表
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyProducts: DataFrame = spark.sql("select productId,count(productId) as count ,yearmonth from " +
      "ratingOfMonth group by yearmonth,productId order by yearmonth desc," +
      "count desc")

    rateMoreRecentlyProducts.write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MORE_RECENTLY_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //3、统计每个商品的平均得分
    /*
    根据历史数据中所有用户对商品的评分，周期性的计算每个商品的平均得分。
实现思路：
通过Spark SQL读取保存在MongDB中的Rating数据集，通过执行以下SQL语句实现对于商品的平均分统计
     */
    val averageProductsDF: DataFrame = spark.sql("select productId,avg(score) as avg from ratings group by " +
      "productId")
    averageProductsDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", AVERAGE_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    spark.stop()

  }
}

//用户评分
case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

//MongoDB配置
case class MongoConfig(uri: String, db: String)
