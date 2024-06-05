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

  //This is a non-distributed (non RDD) function
  def calc_tf(word_list: List[String]) ={
    word_list.map(word => (word -> (word_list.count(x => x == word) / word_list.length)))
  }


  def count_word_in_doc(word: String, doc: List[String]) = {
    doc.count(x => x == word)
  }

  def count_word_in_RDD(word: String, rdd: RDD[(String, String, String, List[String])]) = {
    rdd.aggregate(0, (acc: Int, next: ) => if(next == word){acc+1})
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Test2").setMaster("local[4]")
    val sc = new SparkContext(conf)


    //Put the words into a hashmap and store it in memory, then broadcast it to all spark nodes
    val to_remove = sc.broadcast(Source.fromFile("./remove_words.txt").getLines().toList.map(word => (word -> 1)).toMap)

    var remove_map = to_remove.value

    //line,song_id,artist_id,song,artists,explicit,genres,lyrics
    val lyrics_rdd = sc.textFile("./lyrics_10k.csv")
      //line, song, artists, lyrics
      .map(line => (line.split(",")(0), line.split(",")(3), line.split(",")(4), line.split(",")(7)))
      .map({case (line, song, artists, lyrics) => (line, song, artists,
        lyrics.split(" ").filter(word => !remove_map.contains(word.toLowerCase.replaceAll("[?!a-z]", ""))).mkString(" "))})



    //Build IDF mapping for every document
    val idf_quotient_mapping = lyrics_rdd.map(
      {case (line, song, artists, lyrics) => (line, song, artists,
        lyrics.map(word => (word)))
    })






    lyrics_rdd.saveAsTextFile("./lyrics_filtered")




  }

}