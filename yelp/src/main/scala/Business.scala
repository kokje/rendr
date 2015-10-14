package rendr

case class Restaurant (business_id: String, name: String, city: String, state: String )
object Business {
	def transform(business_id : String, name: String, city: String, state: String, categories: Seq[String]) : Restaurant = {
                // These are all required fields and cannot be null. Record should be discarded in such a case
		if (business_id == null || name == null || state == null || categories == null) {
			Restaurant("-1", "-1", "-1","-1")
		}
		else if (categories.contains("Restaurants")) {
                        Restaurant(business_id, name, city, state)
                }
                else {
                        Restaurant("-1", "-1", "-1","-1")
                }
        }
}
