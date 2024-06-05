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
    //For every word, count how often it apears in a song and divide that by the length of the song
    //Thats the TF
    word_list.map(word => (word, (word_list.count(x => x == word).toDouble / (word_list.length.toDouble))))
  }

  def dot_product(v1: Array[Double], v2: Array[Double]) = {
    for (a <- 0 to v1.length){

    }
  }

  def calc_magnitude(v: Array[Double]) = {
    Math.sqrt(v.map(x => x * x).sum)
  }

  def calc_cosign(v1: Array[Double], v2: Array[Double]) = {
//    v1.map()
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
        lyrics.split(" ")
          //.map(word => word.toLowerCase)//Set every character to a lower case
          //.filter(word => !remove_map.contains(word))
      )}) //This removes the word if its not in the map of "remove words" (stop words)
      .persist(StorageLevel.MEMORY_ONLY)
    //The above simply formats the data from the input file.
    //its new format is (line:String, song:String, artists:String, lyrics: List[String])
    //lyrics is a list of strings. Each string is a word in the lyrics (split on space)

    val num_songs = lyrics_rdd.count() //The number of songs.


    //This removes any information about the artist and song name, we'll have to add that in later
    val lyrics_standalone_rdd = lyrics_rdd.map({case (a, song, c, lyrics) => (song, lyrics.map(x=>x.toLowerCase()))}).persist(StorageLevel.MEMORY_ONLY)


    //  List[  List[(WORD,TF)]  ]
    // This calculate the TF for every song in the lyrics_standalone_rdd list
    val lyrics_freq = lyrics_standalone_rdd.map(
      {case (song, lyrics) => (song, calc_tf(lyrics))}
    )

    //List[Set[String]]
    //This turns every song into a set
    val setOfWords = lyrics_standalone_rdd.map({case (song, lyrics_tf_lst) => (song, lyrics_tf_lst.toSet)})
    //This creates a list of tuples where the key is the word and the value is an initializer 1
    val wordPairs = setOfWords.flatMap({case (song, lyrics_tf_lst) => lyrics_tf_lst.map(word => (word, 1))})
    //This groups every word and when grouped adds together the initalizer
    //For every word, it counts how often it appears in ALL documents
    val document_frequency = wordPairs.reduceByKey(_+_).persist(StorageLevel.MEMORY_ONLY)

    //This calculates the IDF for every word.
    val idf = document_frequency.map({case (word, df) => (word, Math.log(num_songs.toDouble/df.toDouble))})

    //Collect the IDFs as a map so we can reference their values easily
    val idf_map = idf.map({case (key, value) => (key -> value)}).collectAsMap()

    //This iterates over every word in every song
    //For each word, put its TF and IDF next to it
    //List[List[(WORD, TF, IDF)]]
    val word_tf_idf = lyrics_freq
      .map({case (song, lyrics_tf_set) => (song,
        lyrics_tf_set
        .map({case (word, tf) => (word, tf, idf_map.get(word))})
        .map({case (word, tf, Some(df)) => (word, tf, df)})
          )}).collect()

//



    //val freq_map = document_frequency.collect().toMap


    val song_vectors = word_tf_idf.map({case (song, lyrics) => (song,
      lyrics.map({case (word, tf, idf) => (word, (tf * idf))})
    )})




    //Attempt at building the vector space
//    val collected_doc_freq = document_frequency.collect()
//
//    val joined_ = song_vectors.map({case (song, lyrics_tfidf_lst) => (song,
//      {
//        val join_lookup = lyrics_tfidf_lst.toMap
//          collected_doc_freq
//          .map({ case (word, freq) => (join_lookup.get(word))})
//          .map({
//            case (Some(v)) => v
//            case (None) => 0
//          })
//      }
//
//    )})









    joined_.foreach(x => {
      val song_name = x._1
      val song_tf_idf_lst = x._2
      println(song_name)
      song_tf_idf_lst.foreach(println)
    })




  //When you run this you'll notice that the last song has WAYYYY less lyrics than it's supposed to
  //That is because I naively assumed that the CSV would have commas only where we wanted to split the data
  //Thats not the case, it is valid CSV to have ("data", "data", ["data1, "data2"], "data)
  //So simply splitting on commas is not going to give us accurate results

  //However, thats a problem with the data set and its ingress

  //This still calculates TF and IDF correctly.

  }

}