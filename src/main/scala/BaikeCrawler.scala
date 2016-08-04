/* BaikeCrawler.scala */
import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import collection.JavaConverters._
import java.net.URLEncoder
import scala.io.Source
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}


object BaikeCrawler {

  def getCandidateURLs(keyword: String): Iterable[String] = {
    val url = "http://baike.baidu.com/search?word=" + URLEncoder.encode(keyword) + "&pn=0&rn=0&enc=utf8"
    try {
      val page = Source.fromURL(url).mkString
      val body = Jsoup.parse(page).body

      body.getElementsByAttributeValue("class", "result-title").asScala
        .take(10)
        .map(item => item.attr("href"))
    } catch {
      case _: Exception => {
        println(url + " is failed.")
        None
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("crawler").setMaster("local")
    val sc = new SparkContext(conf)


    val companyList = sc.textFile(args(0)).cache()

    val companySize = companyList.count()

    val urllist = companyList.repartition(companySize / 100000 + 1 toInt)
      .flatMap(getCandidateURLs(_))
      .filter(url => url.trim.length > 0 && url.startsWith("http://baike.baidu.com"))
      .distinct()
      .cache()

    print(urllist.collect().mkString("\n"))
    urllist.map(url => {
      try {
        val page = Source.fromURL(url).mkString
        (url, page)
      } catch {
        case _: Exception => {
          println(url + " is failed.")
          (url, None)
        }
      }
    }).filter(item => item._2 != None)
      .saveAsObjectFile(args(1))
  }
}
