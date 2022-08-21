import org.apache.spark.sql.SparkSession
import com.uber.h3core.H3Core
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import scala.sys.process._
import scala.util.{Try, Success, Failure}
import java.text.SimpleDateFormat
import java.util.Date

object Lab1 {
  val geoUDF = udf((lat: Double, lon: Double, res: Int) =>
    h3Helper.toH3func(lat, lon, res)
  )
  val distanceUDF =
    udf((origin: String, des: String) => h3Helper.getH3Distance(origin, des))

  def printConfigs(session: SparkSession) = {
    // get Conf
    val mconf = session.conf.getAll
    //print them
    for (k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}\n") }
  }

  def main(args: Array[String]) {
    // ******** Create a SparkSession  ***************

    val spark = SparkSession
      .builder()
      .appName("Lab 1")
      .config("spark.master", "local")
      //.config("spark.executor.cores", 4)
      //.config("spark.eventlog.logBlockUpdates.enable", true)
      .config("spark.shuffle.file.buffer", "1mb")
      .config("spark.executor.memory", "2g")
      .config("spark.shuffle.unsafe.file.output.buffer", "1mb")
      .config("spark.io.compression.lz4.blockSize", "512kb")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    printConfigs(spark)
    // input check
    val height = typecheck.matchFunc(args(0))
    if (height == -1) {
      println("******************************************************")
      println("Invalid input, program terminated")
      spark.stop
    } else {
      println("******************************************************")
      println("        The sea level rised " + height + " m          ")
      // ************* process osm & alos dataset separately *******************
      val (placeDF, harbourDF) = readOpenStreetMap(
        spark.read.format("orc").load("netherlands-latest.osm.orc")
      ); //complete osm dataset
      val elevationDF = readALOS(spark.read.format("parquet").load("parquet/*")); //complete alos dataset

      // ************** combine two datasets with H3 ************************
      val (floodDF, safeDF) = combineDF(
        placeDF,
        elevationDF.select(col("H3"), col("elevation")),
        height
      )
      // *************** find the closest destination *************
      findClosestDest(floodDF, safeDF, harbourDF)
      // Stop the underlying SparkContext0
      spark.stop

    }

  }
  def readOpenStreetMap(df: DataFrame): (DataFrame, DataFrame) = {
    // ********* explode and filter the useful tags ************
    val splitTagsDF = df
      .filter(col("type") === "node")
      .select(
        col("id"),
        col("lat"),
        col("lon"),
        explode(col("tags"))
      )
      .filter(
        col("key") === "name" || col("key") === "place" ||
          col("key") === "population" || col("key") === "harbour"
      )
    // ********** make the keys to be column names *************
    val groupdf = splitTagsDF
      .groupBy("id", "lat", "lon")
      // This step causes a shuffle
      // groupBy tranformation is a wide transformation
      // It requires data from other partitions to be read, combined and written to disk
      // Each partition may contain data with the same "id", "type", "lat", "lon" value
      .pivot("key", Seq("name", "place", "population", "harbour"))
      .agg(first("value"))

    // ********** remove the rows with imcomplete information *******
    val groupLessDF = groupdf
      .filter(
        (col("place").isNotNull && col("population").isNotNull &&
          (col("place") === "city" || col("place") === "town" || col(
            "place"
          ) === "village" || col("place") === "halmet")) || col(
          "harbour"
        ) === "yes"
      )

    //********** calculate the coarse/fine-grained H3 value ****************
    val h3mapdf = groupLessDF
      .withColumn("H3", geoUDF(col("lat"), col("lon"), lit(7))) //calculate the H3 value for each place
      .cache()   // groupLessDF will be used twice hence should be cached, to avoid to be recomputed when multiple actions are executed 

    //***********separate the harbours and other places *******************
    val harbourDF = h3mapdf
      .filter(col("harbour") === "yes")
      .select(col("H3").as("harbourH3"))

    val placeDF = h3mapdf
      .select(col("name"), col("population"), col("H3"), col("place"),col("id"))
      .filter(col("harbour").isNull)
      .dropDuplicates("id") //id is unique

    println("******************************************************")
    println("* Finished building up DAG for reading OpenStreetMap *")

    return (placeDF, harbourDF)

  }
  def readALOS(alosDF: DataFrame): DataFrame = {

    val h3df_raw = alosDF
      .withColumn("H3", geoUDF(col("lat"), col("lon"), lit(7)))
    val h3df = h3df_raw
      .groupBy("H3") //H3 is now unique
      .min("elevation")
      .withColumnRenamed("min(elevation)", "elevation")
      .select(col("H3"), col("elevation"))
    println("******************************************************")
    println("**** Finished building up DAG for reading ALOSMap ****")
    
    return h3df
  }
  /*combineDF: combine openstreetmap & alos,
           get the relations: name -> lan,lon
           get flooded, safe df
           get the output orc name | evacuees & sum
   */
  def combineDF(
      placeDF: DataFrame,
      elevationDF: DataFrame,
      riseMeter: Int
  ): (DataFrame, DataFrame) = {

    /** ****** Combine osm and alos with h3 value *******
      */
    //combinedDF - name,place,population,H3,min(elevation)

    val combinedDF = placeDF
      .join(elevationDF, Seq("H3"), "inner")
    // This step causes a shuffle
    // The join operation merges two data set over a same matching key
    // It triggers a large amount of data movement across Spark executors

    /** ********split into flood and safe DF **********      */
    //floodDF: place,population, num_evacuees, floodH3
    val floodDF = combinedDF
      .filter(col("elevation") <= riseMeter)
      .drop(
        "elevation",
        "place"
      ) //no need to know the type of flooded place any more
      .withColumnRenamed("population", "num_evacuees")
      .withColumnRenamed("name", "place")
      .withColumnRenamed("H3", "floodH3")
      .withColumn("num_evacuees", col("num_evacuees").cast("int"))
      .cache()  //add floodDF to cache because the computation of floodDF requires shuffle

    /*
   root
  |-- place: string (nullable = true)
  |-- safeH3: string (nullable = true)
  |-- num_evacuees: integer (nullable = true)
  |
     */
    //safeDF - destination,safeH3,safe_population
    // row satisfied:
    // - safe_place == city | harbour

    val safeDF = combinedDF
      .filter(col("elevation") > riseMeter)
      .drop("elevation")
      .filter(col("place") === "city") //the destination must be a city
      .drop("place")
      .withColumnRenamed("population", "safe_population")
      .withColumnRenamed("name", "destination")
      .withColumnRenamed("H3", "safeH3")
      .cache() //add safeDF to cache because the computation of floodDF requires shuffle

    return (floodDF, safeDF)

  }

  def findClosestDest(
      floodDF: DataFrame,
      safeDF: DataFrame,
      harbourDF: DataFrame
  ) {
    val eva_cities = floodDF
      .crossJoin(safeDF) // find all the possible evacuation destinations
      // This step causes a shuffle
      // The join operation merges two data set over a same matching key
      // It triggers a large amount of data movement across Spark executors
      .withColumn("city_distance", distanceUDF(col("floodH3"), col("safeH3")))

    val min_city = eva_cities
      .groupBy("place")
      .agg(
        min("city_distance").as("city_distance") // find the closest safe city
      )

    val closest_city = min_city
      .join(
        eva_cities,
        Seq("place", "city_distance")
      ) 
      .dropDuplicates(
        "place",
        "city_distance"
      ) // avoid duplicate due to the same city_distance

    val closest_harbour = floodDF
      .crossJoin(harbourDF) // find distance to each harbour
      .withColumn(
        "harbour_distance",
        distanceUDF(col("floodH3"), col("harbourH3"))
      )
      .groupBy("place")
      .min("harbour_distance")
      .withColumnRenamed("min(harbour_distance)", "harbour_distance")

    // join closest_city with closest_harbour based on place name
    val floodToSafe = closest_city
      .join(closest_harbour, Seq("place"), "inner")
      .select(
        "place",
        "num_evacuees",
        "harbour_distance",
        "destination",
        "city_distance",
        "safe_population"
      )

    /*
      seperate into two dataframes
      |-- near_harbour: places that are closer to a harbour than a safe city
      |-- near_city: places that are closer to a safe city
     */

    //********** divide into 2 DFs ***********
    val near_harbour = floodToSafe
      .filter(col("harbour_distance") <= col("city_distance"))
      .drop("city_distance", "harbour_distance")
    println("******************************************************")
    println("************ places closer to a harbour **************")
    

    val near_city = floodToSafe
      .filter(col("harbour_distance") > col("city_distance"))
      .drop("harbour_distance", "city_distance")
    println("******************************************************")
    println("************ places closer to a city  ****************")


    // ********* operation on <near_harbour> DF **********
    val change_dest =
      near_harbour.withColumn(
        "destination",
        lit("Waterworld")
      ) // change the destination
    val change_popu = change_dest
      .withColumn("num_evacuees", col("num_evacuees") * 0.25)
      . // evacuees to the WaterWorld
      withColumn(
        "safe_population",
        col("safe_population") * 0
      ) // set the population of WaterWorld to 0
    val rest_popu = near_harbour.withColumn(
      "num_evacuees",
      col("num_evacuees") * 0.75
    ) // evacuees to the nearest city
    val near_harbour_new =
      rest_popu.union(change_popu).sort("place") // Combined DF
    println("******************************************************")
    println("************ evacuees to harbour and city ************")

    val relocate_output =
      near_harbour_new
        .union(near_city)
        .sort("place") // Combine <near_harbour_new> and <near_city>

    println("******************************************************")
    println("************* output => evacuees by place ************")
    println("******************************************************")
    println("******************* Saving data **********************")
    relocate_output
      .drop("safe_population")
      .write
      .mode("overwrite")
      .orc("output/data/relocate.orc") // output as .orc file
    println("******************* Finished save*********************")

    // ********* calculate the total number of evacuees to each destination ********
    println("******************************************************")
    println("****** aggregate evacuees by their destination *******")
    val receive_popu = relocate_output
      .groupBy("destination")
      .agg(
        sum("num_evacuees").as("evacuees_received"),
        avg("safe_population").as("old_population")
      );
    
    /** ******calculate the sum of evacuees*******      */
    println("******************************************************")
    println("********* calculate total number of evacuees *********")
    val sum_popu = receive_popu
      .groupBy()
      .sum("evacuees_received")
      .first
      .get(0)
    println("******************************************************")
    println("|        total number of evacuees is " + sum_popu + "        |")
    println("******************************************************")

    // ******* transform the output data into the required format **********
    val receive_output = receive_popu
      .withColumn(
        "new_population",
        col("old_population") + col("evacuees_received")
      )
      .drop("evacuees_received")

    println("******************************************************")
    println("*** output => population change of the destination ***")
    println("******************************************************")
    println("******************* Saving data **********************")
    receive_output.write
      .mode("overwrite")
      .orc("output/data/receive_output.orc")
    println("******************* Finished save*********************")

  }

}

object h3Helper {
  val h3 = H3Core.newInstance()
  def toH3func(lat: Double, lon: Double, res: Int): String =
    h3.geoToH3Address(lat, lon, res)

  def getH3Distance(origin: String, des: String): Int = {
    return h3.h3Distance(origin, des)
  }
}
