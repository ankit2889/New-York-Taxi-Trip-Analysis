package controllers

import javax.inject._
import play.api._
import play.api.mvc._
import play.api.data._
import play.api.data.Forms._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import com.esri.core.geometry.Point

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject() extends Controller {

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
   
   val form = Form(
    tuple(
      "pickupaddress" -> text,
      "dropoffaddress" -> text,
      "pickupborough" -> text,
      "dropoffborough" -> text,
       "tripTime" ->text
    //   "pickuplatitude" -> text,
    //   "pickuplongitude" -> text,
    //   "dropofflatitude" -> text,
    //   "dropofflongitude" -> text
      
      
    )
  )
  def index = Action {
    Ok(views.html.first())
  }
  
  def first = Action {
      Ok(views.html.first())
  }
  
  def cardPayment = Action {
      Ok(views.html.cardPayment())
  }
  
    def overallBrownie = Action {
      Ok(views.html.overallBrownies())
  }
  
    def scatter = Action {
      Ok(views.html.scatter())
  }
  
    def scatterBrownies = Action {
      Ok(views.html.scatterBrownies())
  }
  
  
  
  
  
  
   def submit = Action { implicit request =>
    val (pickupadress,dropoffaddress,pickupborough,dropoffborough,tripTime) = form.bindFromRequest.get
    val pickuplocation = GetLatitudeAndLongitude.fetchLatitudeAndLongitude(pickupadress)
    val dropofflocation = GetLatitudeAndLongitude.fetchLatitudeAndLongitude(dropoffaddress)
    val (rs,result) = Utilities.getDetails(pickuplocation.get._1,pickuplocation.get._2,dropofflocation.get._1,dropofflocation.get._2,pickupborough.toInt,dropoffborough.toInt,tripTime.toDouble)
    
    Ok("Hi %s %s".format(rs,result))
   
    //Ok("Hi")
  }


}
