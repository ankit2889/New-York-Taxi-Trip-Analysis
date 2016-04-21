package controllers

import spray.json._
import org.joda.time.DateTime
import GeoJsonProtocol._
import org.apache.spark.{SparkContext,SparkConf}
import com.esri.core.geometry.Point
import play.api.Play._
import scala.io.Source
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.joda.time._


object Utilities {
    
  def getDetails(pickUplatitude: Double, pickUplongitude: Double,dropofflatitude: Double,dropofflongitude: Double,pickupBorough:Int,dropoffBorough:Int,tripTime:Double):(String,String) = {    
    
  val conf = new SparkConf().setAppName("Taxi trip analysis2").setMaster("local").set("spark.driver.allowMultipleContexts", "true")
  val sc = new SparkContext(conf)

  val inp = play.Play.application().resource("nyc-boroughs.geojson")
  val geoinp = scala.io.Source.fromURL(inp)
  val geojson = geoinp.mkString
  val features = geojson.parseJson.convertTo[FeatureCollection]



  val areaSortedFeatures = features.sortBy(f => {
    val borough = f("boroughCode").convertTo[Int]
    (borough, -f.geometry.area2D())
  })
  
  def point(longitude: String, latitude: String): Point = {
      new Point(longitude.toDouble, latitude.toDouble)
    }


  val bFeatures = sc.broadcast(areaSortedFeatures)
  
  val pickUploc = new Point(pickUplatitude,pickUplongitude)
  val dropoffloc = new Point(dropofflatitude,dropofflongitude)

  case class Trip(pickupLoc: Point, dropoffLoc: Point)
  val trip = Trip(pickUploc, dropoffloc)

  def borough(trip: Trip): Option[String] = {
    val feature: Option[Feature] = bFeatures.value.find(f => {
      f.geometry.contains(trip.dropoffLoc)
    })
    feature.map(f => {
      f("borough").convertTo[String]
    })
  }

  def boroughPickUp(trip: Trip): Option[String] = {
    val feature: Option[Feature] = bFeatures.value.find(f => {
      f.geometry.contains(trip.pickupLoc)
    })
    feature.map(f => {
      f("borough").convertTo[String]
    })
  }


  def getDayInt(arg:DateTime):Option[Int] ={
    val day = arg.dayOfWeek().get()
    day match {
      case x if (0<=x && x<=7) => Some(x)
      case _ =>None
    }

  }


  def mapBoroughNameToCode(arg:String):Int = {
    arg match {
      case "Manhattan" => 1
      case "Brooklyn" => 2
      case "Queens" => 3
      case "Staten Island" => 4
      case "Bronx" => 5
      case _ => 6


    }
  }

 // val pickupBorough = mapBoroughNameToCode(boroughPickUp(trip).get)
  //val dropOffBorough =mapBoroughNameToCode(borough(trip).get)
  
  
   
  val currentDateTime = new DateTime(DateTimeZone.UTC)

  val currentDay = getDayInt(currentDateTime).get
  val currentSlot =getDayInt(currentDateTime).get

  
  val tripDistance = Haversine.haversine(pickUplatitude.toDouble,pickUplongitude.toDouble,dropofflatitude.toDouble,dropofflongitude.toDouble)
  
  def calculateAmount(tripTime:Double,tripDistance:Double):Double = {

      if((tripDistance< 1.00) && (tripTime < 350))
        return 7.00
      else if(tripDistance < 1.00 && tripTime >= 350)
       return 8.50
      else
       return 15.00

  }
  
  //val tripAmount = tripDistance.toDouble*tripTime*2.5

  val inputData =  Array(pickupBorough,dropoffBorough
                  ,currentDay.toDouble,pickUplatitude.toDouble,pickUplongitude.toDouble,dropofflatitude.toDouble,
                  dropofflongitude.toDouble,tripDistance.toDouble,tripTime.toDouble,calculateAmount(tripTime,tripDistance))


  val parsedInputData = Vectors.dense(inputData)
  val sameModel = LinearRegressionModel.load(sc, "public/myModelPath")

val result = sameModel.predict(parsedInputData)
//  print("Brownie predictability for this ride: "+result)
   
   
 def getResult(arg:Double) :Double = {
     if(result < 1.0)
     return 30.0
     else if(result <=5.0)
     return 40.0
     else if(result > 5.0 && result <=8.0)
     return 50.0
     else if(result > 8.0 && result <= 9.0)
     return 65.0
     else if(result >9.0 && result <=10.0)
     return 75.0
     else 
     return 85.0
 }
   
  
  //(pickupBorough.toString,dropOffBorough.toString)
  ("Brownie predicted is",getResult(result).toString)
  }   
    
}