/**
  * Created by spandanbrahmbhatt on 4/7/16.
  */

import java.text.SimpleDateFormat

import GeoJsonProtocol._
import com.esri.core.geometry.Point
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.joda.time.{DateTime, Duration}
import spray.json._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql._
import java.io._
import org.apache.spark.sql.SQLContext


object Execute extends App {

    val conf = new SparkConf().setAppName("Taxi trip analysis1").setMaster("local")
    val sc = new SparkContext(new SparkConf().setAppName("Taxi trip analysis1").setMaster("local"))
    val taxi = sc.textFile(args(0))

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    def getDateTime(args:String):DateTime = {
      new DateTime((format.parse(args)))
    }




  val taxiPair = taxi.map{t =>
   // NotSerializable notSerializable = new NotSerializable();
    val p = t.split(",")
    ((p(0),getDateTime(p(5))),(p(0),p(1),p(2),p(5),p(3),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13)))
  }



  val fare = sc.textFile(args(1))


  val farePair = fare.map{t =>
    val p = t.split(",")
    ((p(0),getDateTime(p(3))),(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10)))
  }
  //


  val inputJoinedData = taxiPair.join(farePair).values


  inputJoinedData.take(20).foreach(println)
  val inputTaxiData = inputJoinedData.map{t =>
    (t._1._1,t._1._2,t._1._3,t._1._4,t._1._5,
      t._1._6,t._1._7,t._1._8,t._1._9,t._1._10,t._1._11,t._1._12,t._1._13,
      t._2._5,t._2._6,t._2._7,t._2._8,t._2._9,t._2._10,t._2._11)
  }
//


  case class Trip(medallion:String,rate_code:Int,passenger_count:Int,tripTime:Int,tripDistance:Double,
                  pickupTime: DateTime, dropoffTime: DateTime,
                  pickupLoc: Point, dropoffLoc: Point)
  case class Fare(paymentType:String,fareAmount:Double,surcharge:Double,mta_tax:Double,tip_amount:Double,tolls_amount:Double,total_amount:Double)
//
 val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//
  def parse(line: String): (String, Trip,Fare) = {
    val fields = line.split(',')
    val medallion = fields(0)
    val license = fields(1)
    val rate_code = fields(4).toInt
    val passenger_count = fields(6).toInt
    val tripTime = fields(7).toInt
    val tripDistance = fields(8).toDouble
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
    val trip = Trip(medallion,rate_code,passenger_count,tripTime,tripDistance,pickupTime, dropoffTime, pickupLoc, dropoffLoc)
    val fare = Fare(paymentType,fareAmount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount)
    (license, trip,fare)
  }
//
  def point(longitude: String, latitude: String): Point = {
    new Point(longitude.toDouble, latitude.toDouble)
  }

  def hours(trip: Trip): Long = {
    val d = new Duration(
      trip.pickupTime,
      trip.dropoffTime)
    d.getStandardHours
  }

      inputTaxiData.saveAsTextFile("inputTaxiData")
  val joinedInput = sc.textFile("inputTaxiData/part-00000")

  val taxiParse = joinedInput.map(parse)


  println("taxi parse data")

  val taxiClean = taxiParse.filter { case (lic, trip,fare) => {
    val hrs = hours(trip)
    0 <= hrs && hrs < 3
  }
  }

//
  val inp = getClass.getResource("nyc-boroughs.geojson")
  val geoinp = scala.io.Source.fromURL(inp)
  val geojson = geoinp.mkString
  val features = geojson.parseJson.convertTo[FeatureCollection]

 // val p = new Point(-73.994499, 40.75066)
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

  def mapBoroughNameToCode(arg:String):Int = {
    arg match {
      case "Manhattan" => 1
      case "Brooklyn" => 2
      case "Queens" => 3
      case "Staten Island" => 4
      case "Bronx" => 5
      case "No borough" => 6


    }
  }


  def getDay(arg:DateTime):Option[String] ={
    val day = arg.dayOfWeek().get()
    val stringDay = day match {
      case 7 => Some("Sunday")
      case 1 => Some("Monday")
      case 2 => Some("Tuesday")
      case 3 => Some("Wednesday")
      case 4 => Some("Thursday")
      case 5 => Some("Friday")
      case 6 => Some("Saturday")
      case _ => None
    }

    stringDay

  }
//
  def getDayInt(arg:DateTime):Option[Int] ={
    val day = arg.dayOfWeek().get()
    day match {
      case x if (0<=x && x<=7) => Some(x)
      case _ =>None
    }

  }


  def getSlot (arg:DateTime):Option[String] = {
  val slot = arg.hourOfDay().get()
  val slotString = slot match {
    case x if(0<=x && x <=10) => Some("Morning")
    case x if(10<x && x <=16) => Some("Afternoon")
    case x if(16<x && x <=19) => Some("Evening")
    case x if(19<x && x <=24) => Some("Night")

  }
slotString
  }


  def getSlotInt (arg:DateTime):Option[Int] = {
    val slot = arg.hourOfDay().get()
     slot match {
      case x if(0<=x && x <=10) => Some(1)
      case x if(10<x && x <=16) => Some(2)
      case x if(16<x && x <=19) => Some(3)
      case x if(19<x && x <=24) => Some(4)
      case _ => None

    }

  }


  val taxiFinalData = taxiClean.map{ t => (t._1.replace("(",""),t._2.medallion,
    mapBoroughNameToCode(boroughPickUp(t._2).getOrElse("No borough")),mapBoroughNameToCode(borough(t._2).getOrElse("No borough")),
    getDayInt(t._2.dropoffTime).getOrElse(9),getSlotInt(t._2.pickupTime).getOrElse(25),t._2.dropoffTime.toString(),
    t._2.pickupLoc.getX,t._2.pickupLoc.getY, t._2.dropoffLoc.getX,t._2.dropoffLoc.getY,
    t._2.rate_code,t._2.passenger_count,t._2.tripTime,t._2.tripDistance,
    t._3.fareAmount,t._3.mta_tax,t._3.paymentType,t._3.surcharge,t._3.tip_amount,
    t._3.tolls_amount,t._3.total_amount.toString.replace(")",""))}



  taxiFinalData.saveAsTextFile("finalTaxiData")

  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val taxiDataFrame = taxiFinalData.toDF()
  val taxiFinalDataFrame =  taxiDataFrame.withColumnRenamed("_1","hack_license").withColumnRenamed("_2","Medallion")
   .withColumnRenamed("_3","Pickup_borough").withColumnRenamed("_4","Dropoff_borough")
   .withColumnRenamed("_5","Pickup_Day").withColumnRenamed("_6","Pickup_slot")
   .withColumnRenamed("_7","Pickup_datetime")
   .withColumnRenamed("_8","Pickup_latitude").withColumnRenamed("_9","Pickup_longitude")
   .withColumnRenamed("_10","dropoff_latitude").withColumnRenamed("_11","dropoff_longitude")
   .withColumnRenamed("_12","rate_code").withColumnRenamed("_13","passenger_count")
   .withColumnRenamed("_14","tripTime").withColumnRenamed("_15","tripDistance")
   .withColumnRenamed("_16","fareAmount").withColumnRenamed("_17","mta_tax").withColumnRenamed("_18","paymentType")
   .withColumnRenamed("_19","surcharge").withColumnRenamed("_20","tip_amount")
   .withColumnRenamed("_21","tolls_amount").withColumnRenamed("_22","totalAmount")


  // to start with brownie points
  val taxiFinalDataFrameChanged = taxiFinalDataFrame.withColumn("totalAmount",taxiFinalDataFrame.col("totalAmount").cast("int"))
  taxiFinalDataFrameChanged.registerTempTable("taxiTrip")

//  val paymentcrddf = taxiFinalDataFrameChanged.sqlContext.sql("select count(paymentType) as CardPaymentType,Pickup_borough from taxiTrip where paymentType='CRD' GROUP BY Pickup_borough")
//  paymentcrddf.repartition(1).save("com.databricks.spark.csv", SaveMode.ErrorIfExists,Map("path" -> "paymentTypeCRD","header"->"true"))
//
//
//  val paymentcshdf = taxiFinalDataFrameChanged.sqlContext.sql("select count(paymentType) as CashPaymentType,Pickup_borough  from taxiTrip where paymentType='CSH' GROUP BY Pickup_borough")
//  paymentcshdf.repartition(1).save("com.databricks.spark.csv", SaveMode.ErrorIfExists,Map("path" -> "paymentTypeCSH","header"->"true"))

  val browniedf1= taxiFinalDataFrameChanged.sqlContext.sql("select Pickup_Day,Pickup_slot,Pickup_borough,Dropoff_borough," +
    "count(DISTINCT hack_license) as noOfDrivers,ROUND(avg(tripTime),2) as AvgTime, SUM(passenger_count) as PassengerCount," +
    "ROUND(avg(totalAmount),2) as AvgTripFare,max(totalAmount) as MaxTotalAmount," +
    "max(tripTime) as MaxTripTime from taxiTrip where Pickup_borough!='6' AND Dropoff_borough!='6' " +
    "GROUP BY Pickup_Day,Pickup_slot,Pickup_borough,Dropoff_borough")

  browniedf1.repartition(1).save("com.databricks.spark.csv", SaveMode.ErrorIfExists,Map("path" -> "scatterplotAmount","header"->"true"))

  browniedf1.registerTempTable("brownieTable")
  browniedf1.show(50)

  val regex = "[|\\]"
  val a = browniedf1.sqlContext.sql("select max(AvgTime) as MaxAvgTime from brownieTable")
  val MaxAvgTime = a.take(1).apply(0).toString().replaceAll("\\[","").replaceAll("\\]","").toDouble

  val b = browniedf1.sqlContext.sql("select max(AvgTripFare) as MaxAvgTripFare from brownieTable")
  val MaxAvgTripFare = b.take(1).apply(0).toString().replaceAll("\\[","").replaceAll("\\]","").toDouble

  val c = browniedf1.sqlContext.sql("select max(MaxTripTime) from brownieTable")
  val MaxTime = c.take(1).apply(0).toString().replaceAll("\\[","").replaceAll("\\]","").toDouble

  val d = browniedf1.sqlContext.sql("select max(MaxTotalAmount) as MaxAvgTripFare from brownieTable")
  val MaxFare = d.take(1).apply(0).toString().replaceAll("\\[","").replaceAll("\\]","").toDouble

  val browniedf2 = browniedf1.sqlContext.sql("select Pickup_borough,Dropoff_borough,Pickup_Day,Pickup_slot," +
    "ROUND((PassengerCount/noOfDrivers),2) as PassengerDriver,ROUND((AvgTime/"+MaxAvgTime+"),2) as TimeMid," +
    "ROUND((AvgTripFare/"+MaxAvgTripFare+"),2) as FareMid" +
    " from brownieTable GROUP BY Pickup_Day,Pickup_slot,Pickup_borough,Dropoff_borough,PassengerCount,noOfDrivers,AvgTime,AvgTripFare")
  browniedf2.show(20)

  browniedf2.repartition(1).save("com.databricks.spark.csv", SaveMode.ErrorIfExists,Map("path" -> "scatterplotPassengerDriver","header"->"true"))
  browniedf2.registerTempTable("brownieTable2")

  val e = browniedf2.sqlContext.sql("select max(PassengerDriver) as PassengerDriver from brownieTable2")
  val MaxPassengerCount = e.take(1).apply(0).toString().replaceAll("\\[","").replaceAll("\\]","").toDouble

  val browniedf3 = browniedf2.sqlContext.sql("select Pickup_borough,Dropoff_borough,Pickup_Day,Pickup_slot,"+
    "(ROUND((PassengerDriver/"+MaxPassengerCount+"),2)*33) as PassengerDriverBrownie," +
    "ROUND((TimeMid*100),2) as TimeBrownie, ROUND((FareMid*100),2) as FareBrownie" +
    " from brownieTable2 GROUP BY Pickup_Day,Pickup_slot,Pickup_borough,Dropoff_borough,PassengerDriver,TimeMid,FareMid")
  browniedf3.repartition(1).save("com.databricks.spark.csv", SaveMode.ErrorIfExists,Map("path" -> "scatterplotFareTimeBrownie","header"->"true"))
  browniedf3.show(20)



  browniedf3.registerTempTable("brownieFinalTable")

  val sqlQueryProcess = "select T.Pickup_datetime,T.Pickup_borough,T.Dropoff_borough,T.Pickup_Day,T.Pickup_slot,T.tripDistance,T.tripTime,T.totalAmount,T.Pickup_longitude," +
    "T.Pickup_latitude,T.dropoff_longitude,T.dropoff_latitude,B.PassengerDriverBrownie,B.TimeBrownie,B.FareBrownie" +
    " from taxiTrip T, brownieFinalTable B WHERE T.Pickup_borough = B.Pickup_borough" +
    " AND T.Dropoff_borough = B.Dropoff_borough AND" +
    " T.Pickup_Day = B.Pickup_Day AND T.Pickup_slot = B.Pickup_slot"

  val g = taxiFinalDataFrameChanged.sqlContext.sql(sqlQueryProcess)
  g.registerTempTable("finalTable")
  val h = g.sqlContext.sql("select Pickup_borough,Dropoff_borough,Pickup_Day," +
    "Pickup_slot,Pickup_latitude,Pickup_longitude,dropoff_latitude,dropoff_longitude,tripTime,tripDistance," +
    "totalAmount," +
    "(((PassengerDriverBrownie+ROUND(((((tripTime/"+MaxTime+")*100)*TimeBrownie)/300),2))+"
    +"(ROUND(((((totalAmount/"+MaxFare+")*100)*FareBrownie)/300),2)))*2) as OverallBrowniePoint from finalTable")

  h.repartition(1).save("com.databricks.spark.csv", SaveMode.ErrorIfExists,Map("path" -> "regressionInp","header"->"true"))

  val data = sc.textFile("regressionInp/part-00000")
  val filteredData = data.filter(!_.contains("Pickup_latitude"))
  filteredData.first()
  val parsedData = filteredData.map { line =>
    val parts = line.split(',')
    LabeledPoint(parts(10).toDouble, Vectors.dense(parts(0).toDouble,parts(1).toDouble,parts(2).toDouble,
      parts(3).toDouble,parts(4).toDouble,parts(5).toDouble,parts(6).toDouble,parts(7).toDouble,parts(8).toDouble,parts(9).toDouble))
  }.cache()


  // Building the model
  val numIterations = 100
  val stepSize = 0.00001
  val model = LinearRegressionWithSGD.train(parsedData, numIterations,stepSize)


  // Evaluate model on training examples and compute training error
  val valuesAndPreds = parsedData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }

  valuesAndPreds.collect()
  val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
  println("training Mean Squared Error = " + MSE)

  // Save and load model
  model.save(sc, "myModelPath")
  val sameModel = LinearRegressionModel.load(sc, "myModelPath")



















































}
