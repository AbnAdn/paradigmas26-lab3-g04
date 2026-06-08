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

    // Acumuladores
    val feedsSuccess = sc.longAccumulator("feedsOk")
    val feedsFailed  = sc.longAccumulator("feedsFailed")
    val postsSuccess = sc.longAccumulator("postsSuccess")
    val postsFailed  = sc.longAccumulator("postsFailed")
    val postsFilteredAcc = sc.longAccumulator("postsFiltered")

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
      .filter { p =>
        val valid = p.title.trim.nonEmpty && p.selftext.trim.nonEmpty

        if (!valid) postsFilteredAcc.add(1)
        valid
      }
      .cache()

    // Accion terminal
    val t0 = System.currentTimeMillis()
    val totalFilteredPosts = filteredPosts.count()
    val t1 = System.currentTimeMillis()

    // Imprimimos acumuladores
    println(s"Feeds downloaded successfully : ${feedsSuccess.value}")
    println(s"Feeds failed                  : ${feedsFailed.value}")
    println(s"Posts downloaded              : ${postsSuccess.value}")
    println(s"Posts filtered (empty)        : ${postsFilteredAcc.value}")
    println()

    // Paso 4: chequear si hay posts
    if (totalFilteredPosts == 0) {
      println("Error: No valid posts downloaded after filtering")
      filteredPosts.unpersist()
      spark.stop()
      return
    }

    // Paso 5: calcular métricas
    val totalChars   = filteredPosts.map(p => p.title.length + p.selftext.length).sum()
    val avgChars     = totalChars / totalFilteredPosts

    val stats = Map(
      "feedsSuccess"  -> feedsSuccess.value.toInt,
      "feedsFailed"   -> feedsFailed.value.toInt,
      "postsSuccess"  -> postsSuccess.value.toInt,
      "postsFailed"   -> postsFailed.value.toInt,
      "postsFiltered" -> postsFilteredAcc.value.toInt,
      "avgChars"      -> avgChars.toInt
    )

    println(Formatters.formatProcessingStats(stats))
    println()

    // Paso 6: detectar entidades
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)
    val t2 = System.currentTimeMillis()
    val allEntitiesRDD = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }.cache()

    val countRDD = allEntitiesRDD
      .map { e => ((e.entityType, e.text), 1) }           
      .reduceByKey((x, y) => x + y)

    val entityCounts = countRDD.collect().toMap
    val allEntityList = allEntitiesRDD.collect().toList

    val t3 = System.currentTimeMillis()

    println(s"Pipeline stage 1 (filtering + count) : ${(t1 - t0) / 1000.0} s")
    println(s"Pipeline stage 2 (NER + aggregation) : ${(t3 - t2) / 1000.0} s")
    println(s"Total pipeline time                  : ${(t3 - t0) / 1000.0} s")
    println()

    val typeStats = Analyzer.countByType(allEntityList)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    filteredPosts.unpersist()
    allEntitiesRDD.unpersist()

    spark.stop()
  }
}
