// package controllers

// import com.esri.core.geometry.Point
// import org.apache.spark.{SparkContext, SparkConf}
// import org.apache.spark.mllib.linalg.Vectors
// import org.apache.spark.mllib.regression.LinearRegressionModel
// import org.joda.time._
// import GeoJsonProtocol._
// import spray.json._

// //import com.google.gdata.client._

// /**
//   * Created by spandanbrahmbhatt on 4/16/16.
//   */
// object TestLinearRegression extends App{


  

//   val currentDateTime = new DateTime(DateTimeZone.UTC)

//   val currentDay = getDayInt(currentDateTime).get
//   val currentSlot =getDayInt(currentDateTime).get

//   val inputData =  Array(mapBoroughNameToCode(pickupBorough.get),mapBoroughNameToCode(dropOffBorough.get)
//                   ,currentDay.toDouble,pickupLatitude.toDouble,pickupLongitude.toDouble,dropoffLatitude.toDouble,
//                   dropoffLongitude.toDouble,tripDistance.toDouble,tripTime.toDouble,tripAmount.toDouble)


//   val parsedInputData = Vectors.dense(inputData)

// //  val result = Execute.sameModel.predict(parsedInputData)
// //  print("Brownie predictability for this ride: "+result)

// }
