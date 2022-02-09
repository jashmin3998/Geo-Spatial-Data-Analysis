package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  pickupInfo = spark.sql("select x,y,z from pickupInfoView where x>= " + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by z,y,x")
  pickupInfo.createOrReplaceTempView("selectedCellVals")
  // pickupInfo.show()

  pickupInfo = spark.sql("select x, y, z, count(*) as hotCells from selectedCellVals group by x, y, z order by z,y,x")
  pickupInfo.createOrReplaceTempView("selectedCellHotness")
  // pickupInfo.show()
  
  val sumOfSelectedCcells = spark.sql("select sum(hotCells) as sumHotCells from selectedCellHotness")
  sumOfSelectedCcells.createOrReplaceTempView("sumOfSelectedCcells")
  // sumOfSelectedCcells.show()
  
  val mean = (sumOfSelectedCcells.first().getLong(0).toDouble / numCells.toDouble).toDouble
  // println(mean)
  
  spark.udf.register("squared", (inputX: Int) => (((inputX*inputX).toDouble)))
  
  val sumOfSquares = spark.sql("select sum(squared(hotCells)) as sumOfSquares from selectedCellHotness")
  sumOfSquares.createOrReplaceTempView("sumOfSquares")
    // sumOfSquares.show()
  
  val standardDeviation = scala.math.sqrt(((sumOfSquares.first().getDouble(0).toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))).toDouble
  // println(mean)
  
  spark.udf.register("adjacentCells", (inputX: Int, inputY: Int, inputZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.calculateAdjacentCells(inputX, inputY, inputZ, minX, minY, minZ, maxX, maxY, maxZ))))
  
  val adjacentCells = spark.sql("select adjacentCells(sch1.x, sch1.y, sch1.z, " + minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ") as adjacentCellCount,"
  		+ "sch1.x as x, sch1.y as y, sch1.z as z, "
  		+ "sum(sch2.hotCells) as sumHotCells "
  		+ "from selectedCellHotness as sch1, selectedCellHotness as sch2 "
  		+ "where (sch2.x = sch1.x+1 or sch2.x = sch1.x or sch2.x = sch1.x-1) "
  		+ "and (sch2.y = sch1.y+1 or sch2.y = sch1.y or sch2.y = sch1.y-1) "
  		+ "and (sch2.z = sch1.z+1 or sch2.z = sch1.z or sch2.z = sch1.z-1) "
  		+ "group by sch1.z, sch1.y, sch1.x "
  		+ "order by sch1.z, sch1.y, sch1.x")
	adjacentCells.createOrReplaceTempView("adjacentCells")
  // adjacentCells.show()
    
  spark.udf.register("zScore", (adjacentCellCount: Int, sumHotCells: Int, numCells: Int, x: Int, y: Int, z: Int, mean: Double, standardDeviation: Double) => ((HotcellUtils.calculateZScore(adjacentCellCount, sumHotCells, numCells, x, y, z, mean, standardDeviation))))
    
  pickupInfo = spark.sql("select zScore(adjacentCellCount, sumHotCells, "+ numCells + ", x, y, z," + mean + ", " + standardDeviation + ") as getisOrdStatistic, x, y, z from adjacentCells order by getisOrdStatistic desc");
  pickupInfo.createOrReplaceTempView("zScore")
  // pickupInfo.show()
    
  pickupInfo = spark.sql("select x, y, z from zScore")
  pickupInfo.createOrReplaceTempView("finalPickupInfo")
  // pickupInfo.show()

  return pickupInfo
}
}
