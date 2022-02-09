package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val point = pointString.split(",")
    val point_x = point(0).trim().toDouble
    val point_y = point(1).trim().toDouble
    
    val rectangle = queryRectangle.split(",")
    val rectangle_x_1 = rectangle(0).trim().toDouble
    val rectangle_y_1 = rectangle(1).trim().toDouble
    val rectangle_x_2 = rectangle(2).trim().toDouble
    val rectangle_y_2 = rectangle(3).trim().toDouble
    
    var min_x: Double = 0
    var max_x: Double = 0
    if(rectangle_x_1 < rectangle_x_2) {
      min_x = rectangle_x_1
      max_x = rectangle_x_2
    } else {
      min_x = rectangle_x_2
      max_x = rectangle_x_1
    }
    
    var min_y: Double = 0
    var max_y: Double = 0
    if(rectangle_y_1 < rectangle_y_2) {
      min_y = rectangle_y_1
      max_y = rectangle_y_2
    } else {
      min_y = rectangle_y_2
      max_y = rectangle_y_1
    }
    
    if(point_x >= min_x && point_x <= max_x && point_y >= min_y && point_y <= max_y) {
      return true
    } else {
      return false
    }
  }

  // YOU NEED TO CHANGE THIS PART

}
