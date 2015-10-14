package rendr

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import GeoHash._

class TestGeoHash extends FunSuite with ShouldMatchers {
	test("Test encoding") {
	
		// Check exact match
    		var lat = 48.581994 
   		var long = -4.373009
    		encode(lat,long) should equal ("gbsukkpdts04")
	}
	
	test("Test encoding with loose precision") {
		// Change precision of lat/long
		var lat = 48.5
		var long = -4.3
		encode(lat, long) should startWith ("gbs")

		lat = 48.581
        	long = -4.373
        	encode(lat, long) should startWith ("gbsuk")
	}
}
