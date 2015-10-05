package rendr

case class Restaurant (business_id: String,name: String,city: String,state: String,lat: Double,long: Double)
object Business {

// Todo : Don't forget to make this dynamic
        def transform(business_id : String, name: String, city: String, state: String, categories: Seq[String], latitude: Double, longitude: Double) : Restaurant = {
		if (categories.contains("Restaurants")) {
                        Restaurant(business_id, name, city, state, latitude, longitude)
                }
                else {
                        Restaurant("-1", "-1", "-1","-1",0.0, 0.0)
                }
        }

        def filter(restaurant : Restaurant) : Boolean = {
                restaurant.business_id != "-1"
        }
}
