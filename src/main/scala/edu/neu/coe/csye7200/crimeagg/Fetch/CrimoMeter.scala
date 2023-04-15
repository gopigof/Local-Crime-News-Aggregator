import cats.effect._
import org.http4s._
import org.http4s.headers._
import org.http4s.Method._

object CrimeometerAPI {
  def main(args: Array[String]): Unit = {
    val uri = Uri.uri("https://crimeometer.p.rapidapi.com/stats/")
      .withQueryParam("country", "US")
      .withQueryParam("state", "CA")

    val headers = Headers.of(
      Header("X-RapidAPI-Host", "crimeometer.p.rapidapi.com"),
      Header("X-RapidAPI-Key", "YOUR_API_KEY")
    )

    val request = Request[IO](GET, uri, headers = headers)


    }

//    response.unsafeRunSync() match {
//      case Left(error) => println(s"Error: $error")
//      case Right(body) => println(s"Response body: $body")
//    }
  //}
}