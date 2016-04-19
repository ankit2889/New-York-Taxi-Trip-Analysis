/**
  * Created by spandanbrahmbhatt on 4/16/16.
  */


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
object KMeansExample extends App{

  val conf = new SparkConf().setAppName("Taxi trip analysis1").setMaster("local")
  val sc = new SparkContext(conf)
  val taxi = sc.textFile("trip_data_2000.csv")
  taxi.take(10).foreach(println)


  //val taxidata = taxi.filter(_.contains(("2013"))).filter((_.split(",")(11)!="")).filter((_.split(",")(12)!=""))

//  taxidata.take(10)
  //val taxifence = taxi.filter(_.split(",")(10).toDouble>40.70).filter(_.split(",") (11).toDouble<40.86).filter(_.split(",") (12).toDouble>(-74.02)).filter(_.split(",") (13).toDouble<(-73.93))

  println("Taxi fence data")
//  taxifence.take(10).foreach(println)
  val t = taxi.map(line=>Vectors.dense(line.split(',').slice(10,13).map(_.toDouble))).cache()
  println("Count of elem"+t.count())
  val model = KMeans.train(t,3,1)

  val WSSSE = model.computeCost(t)
  println("Within Set Sum of Squared Errors = " + WSSSE)


  val clusterCenters=model.clusterCenters.map(_.toArray)

  clusterCenters.foreach(lines=>println(lines(0),lines(1)))


}
