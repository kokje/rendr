package rendr

case class Restaurant (business_id: String, name: String, city: String, state: String )
object Business {
	def transform(business_id : String, name: String, city: String, state: String, categories: Seq[String]) : Restaurant = {
                if (categories.contains("Restaurants")) {
                        Restaurant(business_id, name, city, state)
                }
                else {
                        Restaurant("-1", "-1", "-1","-1")
                }
        }
}
