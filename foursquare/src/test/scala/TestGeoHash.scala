package rendr

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

class TestGeoHash extends FunSuite with ShouldMatchers {
  import GeoHash._

  test("test encoding") {
	
	// Check exact match
    	val geohash = "gbsukkpdts04"
    	var lat = 48.581994 
   	var long = -4.373009
    	encode(lat,long) should equal (geohash)
	
	// Change precision of lat/long
	lat = 48.5
	long = -4.3
	encode(lat, long) should startWith ("gbs")

	lat = 48.581
        long = -4.373
        encode(lat, long) should startWith ("gbsuk")
	}
}
