import scala.collection.mutable.ListBuffer

case class AddressData(customerId: String,
                       addressId: String,
                       fromDate: Int,
                       toDate: Int)

case class Group(address_id: String, start: Int, end: Int)

object Merger {

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

  def mergeFunctionally(address: String, it: Iterator[AddressData]): List[Group] = {
    val list = it.toList.sortBy(_.fromDate)
    merge(address, list, list.head.fromDate, list.head.toDate)
  }

  private def merge(address: String, ads: List[AddressData], start: Int, end: Int): List[Group] = ads match {
    case x :: rest =>
      if (x.fromDate <= end)
        merge(address, rest, start, end max x.toDate)
      else Group(address, start , end) ::
        merge(address, rest, x.fromDate, x.toDate)
    case Nil => Group(address, start , end) :: Nil
  }

}
