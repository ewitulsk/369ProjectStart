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
  def calc_tf(word_list: Array[String]) = {
    word_list.map(word => (word, 1)).groupBy(_._1).map( { case (key, values) => (key, ((values.map(_._2).sum.toDouble)/word_list.length.toDouble))}  )
  }

  def word_frequencies(word_list: Array[String]) ={
    (word_list.map(word => (word -> (word_list.count(x => x == word))))).toMap
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
        lyrics.split(" ").filter(word => !remove_map.contains(word.toLowerCase.replaceAll("[?!a-z]", ""))))}).persist(StorageLevel.MEMORY_ONLY)





    val lyrics_standalone_rdd = lyrics_rdd.map({case (a, b, c, lyrics) => lyrics.map(x=>x.toLowerCase())})



    //  List[[WORD -> COUNT]]
    val lyrics_freq_map = lyrics_standalone_rdd.map(lyrics => word_frequencies(lyrics)).persist(StorageLevel.MEMORY_ONLY)

    //List[Set[String]]
    val setOfWords = lyrics_standalone_rdd.map(song => song.toSet)
    val wordPairs = setOfWords.flatMap(songSet => songSet.map(word => (word, 1)))
    val idf_quotient = wordPairs.reduceByKey(_+_)

    idf_quotient.foreach(println)

    //lyrics_standalone_rdd.map(x=>x.mkString(" ")).saveAsTextFile("./lyrics_filtered")




  }

}