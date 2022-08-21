import scala.sys.process._
import scala.util.{Try, Success, Failure}


object Typecheck {
  def toInt(s: String): Try[Int] = Try { Integer.parseInt(s.trim) }
  // var height: Int
  def matchFunc(s: String): Int = {
    toInt(s) match {
      case Success(i) =>
        if (!(s.toInt > 0)) {
          println("******************************************************")
          println(" Please enter an positive integer ")
          return -1
        } else {
          println("******************************************************")
          println("      Valid input, start to transform the input stream      ")
          return s.toInt

        }

      case Failure(s) => {
        println("******************************************************")
        println(s"Failed. Reason: $s")
        println("  Please enter an positive integer ")
        return 0

      }
    }
  }

  // val y = for {
  //     a <- toInt(stringA)
  //     b <- toInt(stringB)
  //     c <- toInt(stringC)
  // } yield a + b + c

}