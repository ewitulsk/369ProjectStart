import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel

import scala.collection.{Iterable, _}


object App {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Test2").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val to_remove = Source.fromFile("./remove_words.txt").getLines().toList
    var remove_map = to_remove.map(word => (word -> 1)).toMap

    //line,song_id,artist_id,song,artists,explicit,genres,lyrics
    val lyrics_rdd = sc.textFile("./lyrics_10k.csv")
      //line, song, artists, lyrics
      .map(line => (line.split(",")(0), line.split(",")(3), line.split(",")(4), line.split(",")(7)))
      .map({case (line, song, artists, lyrics) => (//line, song, artists,
        lyrics.split(" ").filter(word => !remove_map.contains(word.toLowerCase.replaceAll("[?!a-z]", ""))).mkString(" "))})

    lyrics_rdd.saveAsTextFile("./lyrics_filtered")




  }

}