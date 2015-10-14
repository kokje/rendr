package rendr

case class Restaurant (business_id: String,name: String,city: String,state: String,lat: Double,long: Double)
object Business {

        def transform(business_id : String, name: String, city: String, state: String, categories: Seq[String], latitude: Double, longitude: Double) : Restaurant = {
		// These are all required fields and the record has to be dropped if any of them is null 
		if (business_id == null || name == null || city == null || state == null || categories == null || latitude == Double.NaN || longitude == Double.NaN) {
			Restaurant("-1", "-1", "-1","-1",0.0, 0.0)
		}
		else if (categories.contains("Restaurants")) {
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
