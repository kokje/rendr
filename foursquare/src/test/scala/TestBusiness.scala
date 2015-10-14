package rendr

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import Business._

class TestBusiness extends FunSuite with ShouldMatchers {
	import GeoHash._

  	test("Test type restaurant") {
		val restaurant = transform("TestBusinessID", "TestRestaurant", "San Francisco", "CA", List("Restaurants"), 43.00, -3.5)
		restaurant.business_id should equal ("TestBusinessID")
	}
	
	test("Test category is not restaurant"){
		val restaurant = transform("TestBusinessID", "TestRestaurant", "San Francisco", "CA", List("Bars"), 43.00, -3.5)
		restaurant.business_id should equal ("-1")
	}
	
	test("Test categories doesn't exist") {
		val restaurant = transform("TestBusinessID", "TestRestaurant","San Francisco", "CA",null, 43.00, -3.5)
                restaurant.business_id should equal ("-1")
	}

	test("Test lat/long not specified, scala casting of ref type") {
		val restaurant = transform("TestBusinessID", "TestRestaurant","San Francisco", "CA",null,null.asInstanceOf[Double], -3.5)
                restaurant.business_id should equal ("-1")
	}
}
