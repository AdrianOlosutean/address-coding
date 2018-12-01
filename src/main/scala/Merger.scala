import scala.collection.mutable.ListBuffer

case class AddressData(customerId: String,
                       addressId: String,
                       fromDate: Int,
                       toDate: Int)

case class Group(address_id: String, start: Int, end: Int)

object Grouper {

  def mergeImperatively(address: String, it: Iterator[AddressData]): List[Group] = {
      val iterator = it.toList.sortBy(_.fromDate).iterator
      val groups: ListBuffer[Group] = new ListBuffer[Group]
      var prev = iterator.next()
      var start = prev.fromDate
      var end = prev.toDate
      while (iterator.hasNext) {
        val current = iterator.next()
        if (current.fromDate <= end) {
          if (current.toDate > end) {
            end = current.toDate
          }
        } else {
          groups += Group(address, start, end)
          start = current.fromDate
          end = current.toDate
        }
        prev = current
      }
      groups.toList
  }

}
