/**
  * Created by spandanbrahmbhatt on 4/18/16.
  */

import com.esri.core.geometry.Point
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest._

class ExecuteSpec extends FlatSpec with Matchers{

  "Execute" should "correctly return the day" in {
    val day = Execute.getDay(new DateTime())
    day should be(Some("Tuesday"))

  }

  it should "return correct day slot" in {
    val slot = Execute.getSlot(new DateTime())
    slot should be (Some("Afternoon"))
  }


  it should "return correct borough mapping" in {
    val borough = Execute.mapBoroughNameToCode("Manhattan")
    borough should be (1)
  }

  it should "return 6 when incorrect borough is supplied" in {
    val borough = Execute.mapBoroughNameToCode("India")
    borough should be (6)
  }










}
