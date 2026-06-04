import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.util.{Try, Success, Failure}

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return
    }

    // Paso 1: cargar suscripciones con manejo de errores
    val subscriptions: List[Subscription] = try {
      FileIO.readSubscriptions(cmdArgs.subscriptionFile).flatten
    } catch {
      case _: java.io.FileNotFoundException =>
        println(s"Error: Could not load ${cmdArgs.subscriptionFile} - file not found")
        spark.stop()
        System.exit(0)
        List.empty
      case _: Exception =>
        println(s"Error: Could not load ${cmdArgs.subscriptionFile} - invalid JSON format")
        spark.stop()
        System.exit(0)
        List.empty
    }

    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      spark.stop()
      System.exit(0)
    }

    // Paso 2: paralelizar y descargar feeds
    val subsParallelized = sc.parallelize(subscriptions)

    val feedsSuccess = sc.longAccumulator("feedsOk")
    val feedsFailed  = sc.longAccumulator("feedsFailed")
    val postsSuccess = sc.longAccumulator("postsSuccess")
    val postsFailed  = sc.longAccumulator("postsFailed")

    val postsRDD: RDD[Post] = subsParallelized.flatMap { sub =>
      try {
        val feedOpt = FileIO.downloadFeed(sub.url)
        feedOpt match {
          case None =>
            feedsFailed.add(1)
            println(s"Warning: Failed to download from '${sub.name}' (${sub.url})")
            Iterator.empty[Post]
          case Some(feed) =>
            feedsSuccess.add(1)
            val posts = JsonParser.parsePosts(feed, sub.name)
            postsSuccess.add(posts.length)
            posts.iterator
        }
      } catch {
        case _: Exception =>
          feedsFailed.add(1)
          println(s"Warning: Failed to download from '${sub.name}' (${sub.url})")
          Iterator.empty[Post]
      }
    }

    // Paso 3: filtrar posts vacíos
    val filteredPosts = postsRDD
      .filter(p => p.title.nonEmpty && p.selftext.nonEmpty)
      .cache()

    val totalFilteredPosts = filteredPosts.count()

    // Paso 4: chequear si hay posts
    if (totalFilteredPosts == 0) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }

    // Paso 5: calcular métricas
    val totalChars   = filteredPosts.map(p => p.title.length + p.selftext.length).sum()
    val avgChars     = totalChars / totalFilteredPosts
    val postsFiltered = postsSuccess.value - totalFilteredPosts

    val stats = Map(
      "feedsSuccess"  -> feedsSuccess.value.toInt,
      "feedsFailed"   -> feedsFailed.value.toInt,
      "postsSuccess"  -> postsSuccess.value.toInt,
      "postsFailed"   -> postsFailed.value.toInt,
      "postsFiltered" -> postsFiltered.toInt,
      "avgChars"      -> avgChars.toInt
    )

    println(Formatters.formatProcessingStats(stats))
    println()

    // Paso 6: detectar entidades
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)
    val countRDD = filteredPosts.flatMap { post =>                                   
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }
    .map { e => ((e.entityType, e.text), 1) }           
    .reduceByKey((x, y) => x + y)                        


    val entityCounts = countRDD.collect().toMap
    val allEntityList = filteredPosts.flatMap { post =>
      Analyzer.detectEntities(post.title + " " + post.selftext, dictionary)
    }.collect().toList
    val typeStats = Analyzer.countByType(allEntityList)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
  }
}
