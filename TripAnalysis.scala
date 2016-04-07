/**
  * Created by spandanbrahmbhatt on 4/7/16.
  */

import com.esri.core.geometry.Point
import org.joda.time.DateTime
import java.text.SimpleDateFormat
import org.joda.time.Duration

object TripAnalysis {

  case class Trip( pickupTime: DateTime, dropoffTime: DateTime, pickupLoc: Point, dropoffLoc: Point)
  val formatter = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss")

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

  def point(longitude: String, latitude: String): Point = { new Point(longitude.toDouble, latitude.toDouble)
  }


  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = { new Function[S, Either[T, (S, Exception)]] with Serializable {
    def apply(s: S): Either[T, (S, Exception)] = { try {
      Left(f(s)) } catch {
      case e: Exception => Right((s, e)) }
    } }
  }

  def hours(trip: Trip): Long = { val d = new Duration(
    trip.pickupTime,
    trip.dropoffTime)
    d.getStandardHours
  }

}
