package rendr

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import Business._

class TestBusiness extends FunSuite with ShouldMatchers {
        
	test("Test type restaurant") {
                // Ideal input
                val restaurant = transform("TestBusinessID", "TestRestaurant", "San Francisco", "CA", List("Restaurants"))
                restaurant.business_id should equal ("TestBusinessID")
        }

        test("Test category is not restaurant"){
                val restaurant = transform("TestBusinessID", "TestRestaurant", "San Francisco", "CA", List("Bars"))
                restaurant.business_id should equal ("-1")
        }

        test("Test categories doesn't exist") {
                val restaurant = transform("TestBusinessID", "TestRestaurant","San Francisco", "CA",null)
                restaurant.business_id should equal ("-1")
        }
}

