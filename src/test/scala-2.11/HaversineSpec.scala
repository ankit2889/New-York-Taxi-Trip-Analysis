
import com.esri.core.geometry.Point
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest._

class HaversineSpec extends FlatSpec with Matchers {



  "Haversine" should "correctly return the distance" in {
    val distance = Haversine.haversine(36.12, -86.67, 33.94, -118.40)
    distance should be (2887.2599506071106)

  }

  it should "correctly return the zero distance" in {
    val distance = Haversine.haversine(0,0,0,0)
    distance should be (0)

  }

}