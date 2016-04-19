/**
  * Created by spandanbrahmbhatt on 4/19/16.
  */

import spray.json._
import org.joda.time.DateTime
import GeoJsonProtocol._
import org.apache.spark.{SparkContext,SparkConf}
import com.esri.core.geometry.Point
//import play.api.Play._
import scala.io.Source
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.joda.time._
object TestLinear extends App{

    val conf = new SparkConf().setAppName("Taxi trip analysis2").setMaster("local").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    println("Enter borough code")
    val pickupBorough = readDouble()
    val dropoffBorough = readDouble()

    val currentDateTime = new DateTime(DateTimeZone.UTC)

    val currentDay = Execute.getDayInt(currentDateTime).get
    val currentSlot =Execute.getDayInt(currentDateTime).get

    println("Enter location details")

    val pickUplatitude = readDouble()
    val pickUplongitude = readDouble()
    val dropofflatitude = readDouble()
    val dropofflongitude = readDouble()


    val tripDistance = Haversine.haversine(pickUplatitude,pickUplongitude,dropofflatitude,dropofflongitude)

    println("trip distance is"+tripDistance)

    println("Enter trip time")

    val tripTime = readDouble()


    def calculateAmount(tripTime:Double,tripDistance:Double):Double = {

      if((tripDistance< 1.00) && (tripTime < 350))
        return 7.00
      else if(tripDistance < 1.00 && tripTime >= 350)
       return 8.50
      else
       return 15.00

  }

    val inputData =  Array(pickupBorough,dropoffBorough
    ,currentDay.toDouble,pickUplatitude.toDouble,pickUplongitude.toDouble,dropofflatitude.toDouble,
    dropofflongitude.toDouble,tripDistance.toDouble,tripTime.toDouble,calculateAmount(tripTime,tripDistance))


  val parsedInputData = Vectors.dense(inputData)
  val sameModel = LinearRegressionModel.load(sc, "myModelPath")

  val result = sameModel.predict(parsedInputData)
   print("Brownie predictability for this ride: "+result)

}
