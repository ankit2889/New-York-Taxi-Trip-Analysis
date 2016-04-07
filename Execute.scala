/**
  * Created by spandanbrahmbhatt on 4/7/16.
  */

import java.text.SimpleDateFormat


import com.esri.core.geometry.Point
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.joda.time.{Duration, DateTime}
import spray.json._
import GeoJsonProtocol._


object Execute extends App {


  case class Trip(pickupTime: DateTime, dropoffTime: DateTime, pickupLoc: Point, dropoffLoc: Point)

  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def parse(line: String): (String, Trip) = {
    val fields = line.split(',')
    val license = fields(1)
    val pickupTime = new DateTime(formatter.parse(fields(5)))
    val dropoffTime = new DateTime(formatter.parse(fields(6)))
    val pickupLoc = point(fields(10), fields(11))
    val dropoffLoc = point(fields(12), fields(13))
    val trip = Trip(pickupTime, dropoffTime, pickupLoc, dropoffLoc)
    (license, trip)
  }

  def point(longitude: String, latitude: String): Point = {
    new Point(longitude.toDouble, latitude.toDouble)
  }


  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
  }

  def hours(trip: Trip): Long = {
    val d = new Duration(
      trip.pickupTime,
      trip.dropoffTime)
    d.getStandardHours
  }

  val conf = new SparkConf().setAppName("Taxi trip analysis").setMaster("local")
  val sc = new SparkContext(conf)
  val taxi = sc.textFile("trip_data_1000.csv")
  taxi.take(5).foreach(println)


  val safeParse = safe(parse)
  val taxiParse = taxi.map(safeParse)
  taxiParse.cache()


  taxiParse.map(_.isLeft).countByValue().foreach(println)



  val taxiBad = taxiParse.collect({ case t if t.isRight => t.right.get
  })


  val taxiGood = taxiParse.collect({ case t if t.isLeft => t.left.get
  })
  taxiGood.cache()


  taxiGood.values.map(hours).
    countByValue().
    toList.
    sorted.
    foreach(println)


  val taxiClean = taxiGood.filter { case (lic, trip) => {
    val hrs = hours(trip)
    0 <= hrs && hrs < 3
  }
  }


  val inp = getClass.getResource("nyc-boroughs.geojson")
  val geoinp = scala.io.Source.fromURL(inp)
  val geojson = geoinp.mkString
  val features = geojson.parseJson.convertTo[FeatureCollection]

  val p = new Point(-73.994499, 40.75066)
  //val borough = features.find(f => f.geometry.contains(p))

  val areaSortedFeatures = features.sortBy(f => {
    val borough = f("boroughCode").convertTo[Int]
    (borough, -f.geometry.area2D())
  })


  val bFeatures = sc.broadcast(areaSortedFeatures)

  def borough(trip: Trip): Option[String] = {
    val feature: Option[Feature] = bFeatures.value.find(f => {
      f.geometry.contains(trip.dropoffLoc)
    })
    feature.map(f => {
      f("borough").convertTo[String]
    })
  }

  println("Before climax")

  def getDay(arg:DateTime):String ={
    val day = arg.dayOfWeek().get()
    val stringDay = day match {
      case 7 => "Sunday"
      case 1 => "Monday"
      case 2 => "Tuesday"
      case 3 => "Wednesday"
      case 4 => "Thursday"
      case 5 => "Friday"
      case 6 => "Saturday"
    }

    stringDay

  }


  def getSlot (arg:DateTime):String = {
  val slot = arg.hourOfDay().get()
  val slotString = slot match {
    case x if(0<=x && x <=10) => "Morning"
    case x if(10<x && x <=16) => "Afternoon"
    case x if(16<x && x <=19) => "Evening"
    case x if(19<x && x <=24) => "Night"

  }
slotString
  }



 // taxiClean.take(100).foreach(println)

val taxiFinalData = taxiClean.map{ t => (t._1,t._2.pickupTime,getDay(t._2.dropoffTime),getSlot(t._2.pickupTime),t._2.dropoffTime,
  t._2.pickupLoc.getX,t._2.pickupLoc.getY,
  t._2.dropoffLoc.getX,t._2.dropoffLoc.getY,borough(t._2).getOrElse("No borough"))}
//taxiFinalData.take(800).foreach(println)

taxiFinalData.saveAsTextFile("sampleinput")
//  taxiClean.values.
//    map(borough).
//    countByValue().
//    foreach(println)


}
