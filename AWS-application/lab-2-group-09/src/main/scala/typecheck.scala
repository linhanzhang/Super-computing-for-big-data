import scala.sys.process._
import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession

object typecheck {
  def toInt(s: String): Try[Int] = Try { Integer.parseInt(s.trim) }
  // var height: Int
  def matchFunc(s: String): Int = {
    toInt(s) match {
      case Success(i) =>
        // The highest elevation of a city in the Netherlands is 96 meters.
        // In this case, if the sea level is above 96 meters, there will be no
        // safe place to go.
        // So a valid input should between 0 ~ 8848
        if (!(s.toInt >= 0) || !(s.toInt < 8849)) {
          println("******************************************************")
          println(" Please enter an integer between 0 ~ 96(not included) ")
          return -1
        } else {
          println("******************************************************")
          println("      Valid input, calculating evacuation plan      ")
          return s.toInt

        }

      case Failure(s) => {
        println("******************************************************")
        println(s"Failed. Reason: $s")
        println("  Please enter an integer between 0 ~ 96(not included) ")
        return 0

      }
    }
  }
}
