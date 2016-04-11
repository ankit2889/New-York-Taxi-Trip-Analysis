/**
  * Created by spandanbrahmbhatt on 4/7/16.
  */

import java.text.SimpleDateFormat

import GeoJsonProtocol._
import com.esri.core.geometry.Point
import org.apache.spark.SparkConf
import org.joda.time.{DateTime, Duration}
import spray.json._
import org.apache.spark.SparkContext


import org.apache.spark.sql.SQLContext





object Execute extends App {

    val conf = new SparkConf().setAppName("Taxi trip analysis1").setMaster("local")
    val sc = new SparkContext(conf)
    val taxi = sc.textFile("trip_data_2000.csv").distinct()

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    def getDateTime(args:String):DateTime = {
      new DateTime((format.parse(args)))
    }



  val taxiPair = taxi.map{t =>
    val p = t.split(",")
    ((p(0),getDateTime(p(5))),(p(0),p(1),p(2),p(5),p(3),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13)))
  }



  val fare = sc.textFile("trip_fare_2000.csv")


  val farePair = fare.map{t =>
    val p = t.split(",")
    ((p(0),getDateTime(p(3))),(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10)))
  }
  //


  val inputJoinedData = taxiPair.join(farePair).values

  println(inputJoinedData.count())
  inputJoinedData.take(20).foreach(println)
  val inputTaxiData = inputJoinedData.map{t =>
    (t._1._1,t._1._2,t._1._3,t._1._4,t._1._5,
      t._1._6,t._1._7,t._1._8,t._1._9,t._1._10,t._1._11,t._1._12,t._1._13,
      t._2._5,t._2._6,t._2._7,t._2._8,t._2._9,t._2._10,t._2._11)
  }
//
//  removeAll("inputTaxiData")
//  removeAll("finalInputData")
 // inputTaxiData.take(20).foreach(println)

  case class Trip(pickupTime: DateTime, dropoffTime: DateTime, pickupLoc: Point, dropoffLoc: Point)
  case class Fare(paymentType:String,fareAmount:Double,surcharge:Double,mta_tax:Double,tip_amount:Double,tolls_amount:Double,total_amount:Double)
//
 val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//
  def parse(line: String): (String, Trip,Fare) = {
    val fields = line.split(',')
    val license = fields(1)
    val pickupTime = new DateTime(formatter.parse(fields(3)))
    val dropoffTime = new DateTime(formatter.parse(fields(5)))
    val pickupLoc = point(fields(9), fields(10))
    val dropoffLoc = point(fields(11), fields(12))
    val paymentType = fields(13)
    val fareAmount = fields(14).toDouble
    val surcharge = fields(15).toDouble
    val mta_tax = fields(16).toDouble
    val tip_amount = fields(17).toDouble
    val tolls_amount = fields(18).toDouble
    val total_amount = fields(19).replace(")","")toDouble
    val trip = Trip(pickupTime, dropoffTime, pickupLoc, dropoffLoc)
    val fare = Fare(paymentType,fareAmount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount)
    (license, trip,fare)
  }
//
  def point(longitude: String, latitude: String): Point = {
    new Point(longitude.toDouble, latitude.toDouble)
  }
//
//


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
//
  def hours(trip: Trip): Long = {
    val d = new Duration(
      trip.pickupTime,
      trip.dropoffTime)
    d.getStandardHours
  }


  val safeParse = safe(parse)

  inputTaxiData.saveAsTextFile("inputTaxiData")
  val joinedInput = sc.textFile("inputTaxiData/part-00000")

  val taxiParse = joinedInput.map(parse)


  println("taxi parse data")
  taxiParse.take(10).foreach(println)


  taxiParse.cache()

//
//
  val taxiClean = taxiParse.filter { case (lic, trip,fare) => {
    val hrs = hours(trip)
    0 <= hrs && hrs < 3
  }
  }
//
//
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

  def boroughPickUp(trip: Trip): Option[String] = {
    val feature: Option[Feature] = bFeatures.value.find(f => {
      f.geometry.contains(trip.pickupLoc)
    })
    feature.map(f => {
      f("borough").convertTo[String]
    })
  }


//
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





val taxiFinalData = taxiClean.map{ t => (t._1.replace("(",""),borough(t._2).getOrElse("No borough"),boroughPickUp(t._2).getOrElse("No borough"),getDay(t._2.dropoffTime),getSlot(t._2.pickupTime),t._2.dropoffTime,
  t._2.pickupLoc.getX,t._2.pickupLoc.getY,
  t._2.dropoffLoc.getX,t._2.dropoffLoc.getY,t._3.fareAmount,t._3.mta_tax,t._3.paymentType,t._3.surcharge,t._3.tip_amount,
  t._3.tolls_amount,t._3.total_amount.toString.replace(")",""))}




  taxiFinalData.saveAsTextFile("finalInputData")

  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._


  

  val taxiDataFrame = taxiFinalData.toDF()

  taxiDataFrame.show(5)
  // to start with brownie points









}
