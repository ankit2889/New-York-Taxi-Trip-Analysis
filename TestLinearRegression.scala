//
//import com.esri.core.geometry.Point
//import org.apache.spark.{SparkContext, SparkConf}
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.LinearRegressionModel
//import org.joda.time._
//import GeoJsonProtocol._
//import spray.json._
//
////import com.google.gdata.client._
//
///**
//  * Created by spandanbrahmbhatt on 4/16/16.
//  */
//object TestLinearRegression extends App{
//
//
//  val conf = new SparkConf().setAppName("Taxi trip analysis2").setMaster("local").set("","true")
//  val sc = new SparkContext(conf)
//
//
//
//  val pickupAddress = readLine("Enter pickup address")
//
//

//    def point(longitude: String, latitude: String): Point = {
//      new Point(longitude.toDouble, latitude.toDouble)
//    }
//  val pickupLatitude = readDouble()
//  println(pickupLatitude+":pickup latitude")
//  val pickupLongitude = GetLatitudeAndLongitude.fetchLatitudeAndLongitude(pickupAddress).get._2
//  println(pickupLongitude+":pickup longitude")
//
//
//
//  val dropoffAddress = readLine("Enter dropoff address")
//
//  val dropoffLatitude = GetLatitudeAndLongitude.fetchLatitudeAndLongitude(dropoffAddress).get._1
//  println(dropoffLatitude+":dropoff latitude")
//  val dropoffLongitude = GetLatitudeAndLongitude.fetchLatitudeAndLongitude(dropoffAddress).get._2
//  println(dropoffLongitude+":dropoff latitude")
//
//
//  val pickUploc = point(pickupLongitude.toString,pickupLatitude.toString)
//  val dropoffloc = point(dropoffLongitude.toString,dropoffLatitude.toString)
//
//
//  val inp = getClass.getResource("nyc-boroughs.geojson")
//  val geoinp = scala.io.Source.fromURL(inp)
//  val geojson = geoinp.mkString
//  val features = geojson.parseJson.convertTo[FeatureCollection]
//
//
//
//  val areaSortedFeatures = features.sortBy(f => {
//    val borough = f("boroughCode").convertTo[Int]
//    (borough, -f.geometry.area2D())
//  })
//
//
//  val bFeatures = sc.broadcast(areaSortedFeatures)
//
//  case class Trip(pickupLoc: Point, dropoffLoc: Point)
//  val trip = Trip(pickUploc, dropoffloc)
//
//  def borough(trip: Trip): Option[String] = {
//    val feature: Option[Feature] = bFeatures.value.find(f => {
//      f.geometry.contains(trip.dropoffLoc)
//    })
//    feature.map(f => {
//      f("borough").convertTo[String]
//    })
//  }
//
//  def boroughPickUp(trip: Trip): Option[String] = {
//    val feature: Option[Feature] = bFeatures.value.find(f => {
//      f.geometry.contains(trip.pickupLoc)
//    })
//    feature.map(f => {
//      f("borough").convertTo[String]
//    })
//  }
//
//
//  def getDayInt(arg:DateTime):Option[Int] ={
//    val day = arg.dayOfWeek().get()
//    day match {
//      case x if (0<=x && x<=7) => Some(x)
//      case _ =>None
//    }
//
//  }
//
//
//  def mapBoroughNameToCode(arg:String):Int = {
//    arg match {
//      case "Manhattan" => 1
//      case "Brooklyn" => 2
//      case "Queens" => 3
//      case "Staten Island" => 4
//      case "Bronx" => 5
//      case "No borough" => 6
//
//
//    }
//  }
//
//  val pickupBorough = boroughPickUp(trip)
//  val dropOffBorough = borough(trip)
//
//
//  val tripDistance = Haversine.haversine(pickupLatitude,pickupLongitude,dropoffLatitude,dropoffLongitude).toString
//
//  println("Enter trip time")
//  val tripTime = readDouble()
//  val tripAmount = tripDistance.toDouble*tripTime*2.5
//
//
//  val currentDateTime = new DateTime(DateTimeZone.UTC)
//
//  val currentDay = getDayInt(currentDateTime).get
//  val currentSlot =getDayInt(currentDateTime).get
//
//  val inputData =  Array(mapBoroughNameToCode(pickupBorough.get),mapBoroughNameToCode(dropOffBorough.get)
//                  ,currentDay.toDouble,pickupLatitude.toDouble,pickupLongitude.toDouble,dropoffLatitude.toDouble,
//                  dropoffLongitude.toDouble,tripDistance.toDouble,tripTime.toDouble,tripAmount.toDouble)
//
//
//  val parsedInputData = Vectors.dense(inputData)
//
////  val result = Execute.sameModel.predict(parsedInputData)
////  print("Brownie predictability for this ride: "+result)
//
//}
