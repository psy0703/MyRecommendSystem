package com.ng.streaming

import java.io

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 商品实时推荐
  * （1）用户u 对商品p 进行了评分，触发了实时推荐的一次计算；
  * （2）选出商品p 最相似的K 个商品作为集合S；
  * （3）获取用户u 最近时间内的K 条评分，包含本次评分，作为集合RK；
  * （4）计算商品的推荐优先级，产生<qID,>集合updated_S；
  * 将updated_S 与上次对用户u 的推荐结果Rec 利用公式(4-4)进行合并，产生新的推荐结果NewRec；作为最终输出。
  *
  * @Author: Cedaris
  * @Date: 2019/7/2 15:46
  */
object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("127.0.0.1")
  lazy val mongoClient = MongoClient(MongoClientURI
  ("mongodb://psy833:27017/recommender"))
}

case class MongoConfig(uri:String,db:String)
//标准推荐
case class Recommendation(productId:Int,score:Double)
//用户的推荐
case class UserRecs(userId:Int,recs:Seq[Recommendation])
//商品的相似度
case class ProductRecs(productId:Int,recs:Seq[Recommendation])

object StreamingRecommender {

  //用户评分列表长度
  val MAX_USER_RATING_NUM = 20
  //相似商品列表长度
  val MAX_SIM_PRODUCTS_NUM = 20
  //实时推荐结果保存
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  //用户对商品评分
  val MONGODB_RATING_COLLECTION = "Rating"
  //商品相似度
  val MONGODB_PRODUCT_RECS_COLLECTION = "ProductRecs"

  def main(args: Array[String]): Unit = {
    val config: Map[String, String] = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://psy833:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores"))
      .setAppName("StreamingRecommender")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val ssc = new StreamingContext(sc,Seconds(2))

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo" +
      ".db"))

    import spark.implicits._

    //广播商品相似度矩阵，转换成Map[Int,Map[Int,Double]]
    val simProductsMatrix: collection.Map[Int, Map[Int, Double]] = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      .map { recs =>
        (recs.productId, recs.recs.map(x => (x.productId, x.score)).toMap)
      }.collectAsMap()
    //广播
    val simProductsMatrixBroadCast: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(simProductsMatrix)

    //获取kafka数据
    //创建到Kafka的链接
    val kafkaParams: Map[String, io.Serializable] = Map(
      "bootstrap.servers" -> "psy831:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )
    //获取Kafka数据流
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")),
        kafkaParams)
    )
    //产生评分流
    //UID|MID|SCORE|TIMESTAMP
    val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map { case msg =>
      val attr: Array[String] = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    // 核心实时推荐算法
    ratingStream.foreachRDD{ rdd =>
      rdd.map{case (userId,productId,score,timestamp) =>
        println(">>>>>>>>>>>>>")
        //TODO:获取当前最近的M次商品评分
        val userRecentlyRatings: Array[(Int, Double)] = getUserRecentlyRating(MAX_USER_RATING_NUM,userId,ConnHelper.jedis)

        //TODO:获取商品P最相似的K个商品
        val simProducts: Array[Int] = getTopSimProducts(MAX_SIM_PRODUCTS_NUM,productId,userId,
          simProductsMatrixBroadCast.value)

        //TODO:计算待选商品的推荐优先级
        val streamRecs: Array[(Int, Double)] = computeProductScores(simProductsMatrixBroadCast.value,
          userRecentlyRatings,simProducts)

        //TODO:将数据保存到MongoDB
        saveRecsToMongoDB(userId,streamRecs)
      }.count()
    }

    //启动streaming程序
    ssc.start()
    ssc.awaitTermination()
  }
  import scala.collection.JavaConversions._
  /*
  业务服务器在接收用户评分的时候，默认会将该评分情况以userId, productId, rate, timestamp的格式插入到Redis中该用户对应的队列当中，
  在实时算法中，只需要通过Redis客户端获取相对应的队列内容即可
   */
  /**
    * 获取用户的K次最近评分
    * @param num 获取列表长度
    * @param userId 需要获取的用户id
    * @param jedis redis客户端
    * @return
    */
  def getUserRecentlyRating(num:Int,userId:Int,jedis: Jedis):Array[(Int,Double)]={
    //从用户的队列中取出num个评分
    jedis.lrange("userid:" + userId.toString,0,num).map{
      item => {
        val attr: Array[String] = item.split("\\:")
        (attr(0).trim.toInt,attr(1).trim.toDouble)
      }
    }.toArray
  }
  /**
    * 获取当前商品K个相似的商品
    * @param num          相似商品的数量
    * @param productId    当前商品的ID
    * @param userId       当前的评分用户
    * @param simProducts  商品相似度矩阵的广播变量值
    * @param mongConfig   MongoDB的配置
    * @return
    */
  def getTopSimProducts(num:Int, productId:Int, userId:Int,
                        simProducts:scala.collection.Map[Int,scala.collection
                        .immutable.Map[Int,Double]])(implicit mongConfig: MongoConfig): Array[Int] ={
    //从广播变量的商品相似度矩阵中获取当前商品所有的相似商品
    val allSimProducts = simProducts.get(productId).get.toArray
    //获取用户已经观看过的商品
    val ratingExist = ConnHelper.mongoClient(mongConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("userId" -> userId))
      .toArray
      .map{item =>
      item.get("productId").toString.toInt
    }
    //过滤掉已经评分过得商品，并排序输出
    allSimProducts
      .filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)
  }

  /**
    * 计算待选商品的推荐分数
    *
    * @param simProcts           商品相似度矩阵
    * @param userRecentlyRatings 用户最近的k次评分
    * @param topSimProducts      当前商品最相似的K个商品
    * @return
    */
  def computeProductScores(simProcts:scala.collection.Map[Int,scala
  .collection.immutable.Map[Int,Double]],userRecentlyRatings:Array[(Int,
    Double)],topSimProducts:Array[Int]):Array[(Int,Double)]={

    //用于保存每一个待选商品和最近评分的每一个商品的权重得分
    val score: ArrayBuffer[(Int, Double)] = scala.collection.mutable.ArrayBuffer[(Int,Double)]()

    //用于保存每一个商品的增强因子数
    val increMap: mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap[Int,Int]()
    //用于保存每一个商品的减弱因子
    val decreMap: mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap[Int,Int]()

    for(topSimProduct <- topSimProducts;userRecentlyRating <-
    userRecentlyRatings){
      //获取当个商品之间的相似度
      val simScore: Double = getProductsSimScore(simProcts,userRecentlyRating._1,topSimProduct)
      if(simScore > 0.6){
        score += ((topSimProduct,simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3){
          increMap(topSimProduct) = increMap.getOrDefault(topSimProduct,0) + 1
        }else{
          decreMap(topSimProduct) = decreMap.getOrDefault(topSimProduct,0) + 1
        }
      }
    }

    score.groupBy(_._1).map{
      case (productId,sims) => {
        //log 实现为取10的对数（常用对数）
        (productId,sims.map(_._2).sum / sims.length + log(increMap
          .getOrDefault(productId,1)) - log(decreMap.getOrDefault(productId,1)))
      }
    }.toArray.sortWith(_._2 > _._2)

  }
  /**
    * 获取当个商品之间的相似度
    * @param simProducts       商品相似度矩阵
    * @param userRatingProduct 用户已经评分的商品
    * @param topSimProduct     候选商品
    * @return
    */
  def getProductsSimScore(simProducts:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],
                          userRatingProduct:Int, topSimProduct:Int): Double = {
    simProducts.get(topSimProduct) match {
      case Some(sim) => sim.get(userRatingProduct) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }
  //取10的对数
  def log(m:Int):Double ={
    math.log(m) / math.log(10)
  }

  def saveRecsToMongoDB(userId:Int,streamRecs:Array[(Int,Double)])(implicit
                                                                   mongoConfig: MongoConfig): Unit ={
    //到Streamrecs的链接
    val streamRecsColletion: MongoCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
    //先删除之前的记录
    streamRecsColletion.findAndRemove(MongoDBObject("userId" -> userId))
    //插入新的记录
    streamRecsColletion.insert(MongoDBObject("userId" -> userId,"recs" ->
      streamRecs.map(x => MongoDBObject("productId" -> x._1,"score" -> x._2))))
  }

}
/**
  * 实时推荐算法的前提：
  * 1.	在Redis集群中存储了每一个用户最近对商品的K次评分。实时算法可以快速获取。
  * 2.	离线推荐算法已经将商品相似度矩阵提前计算到了MongoDB中。
  * 3.	Kafka已经获取到了用户实时的评分数据
  * 算法过程如下：
  * 实时推荐算法输入为一个评分<userId, productId, rate, timestamp>，
  * 而执行的核心内容包括：
  * 获取userId 最近K 次评分、
  * 获取productId 最相似K 个商品、
  * 计算候选商品的推荐优先级、
  * 更新对userId 的实时推荐结果。
  */