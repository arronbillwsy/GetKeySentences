import org.apache.spark.sql.SparkSession

object getKeySentences {

//  val conf = new SparkConf().setAppName("get_key_sen").setMaster("local")
//  .set("spark.driver.memory", "4g").set("spark.executor.memory", "4g");
//  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().master("local").appName("get_key_sen")
//    .config("spark.some.config.option", "some-value")
    .config("spark.executor.memory", "14g")
    .getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext




  def main(args: Array[String]): Unit = {

    val i = 0;
    for( i <- 0 to 8) {
      //      val wordFile = s"file:///C:/Users/31476/Desktop/543/bytecup2018/bytecup.corpus.train.${i}.txt"
      val wordFile = s"file:////media/wsy/DATA/Data/preprocess_data/processed_train.${i}.txt"
      val outputPath = s"file:////media/wsy/DATA/Data/preprocess_data/processed_key_sen_train.${i}.txt"
      var data = readContent(wordFile)
      val df = data.map(r => (textRank(r.get(0).asInstanceOf[Seq[Seq[String]]]),r.getLong(1),r.getString(2)))
//      df.write.json(outputPath)
      df.show(1)
      print(1)
    }
  }


  def readContent(path : String) ={
    val stringFrame = spark.read.text(path).as[String]
    val jsonFrame = spark.read.json(stringFrame)
    //    val data = jsonFrame.select("id","title", "content")
    //    data
    jsonFrame
  }

  def textRank(sentences:Seq[Seq[String]]) : Seq[Seq[String]] ={
    val top_k = 10
    val threshhold = 0.1
    val num_iteration = 100
    val i = 0
    var edges: Array[(Seq[String],Seq[String])] = Array()
    var ranks: Array[(Seq[String],Double)] = Array()
    for (i <- 0 until sentences.length){
      val s1 = sentences(i)
      ranks :+= (s1,1/sentences.length.toDouble)
      val j=0
      for (j <- 0 until sentences.length){
        if (i!=j){
          val s2 = sentences(j)
          val sim = cal_sen_similarity(s1,s2)
          if (sim>threshhold){
            edges :+= (s1,s2)
            edges :+= (s2,s1)
          }
        }
      }
    }
    var rRDD = sc.parallelize(ranks)
    var eRDD = sc.parallelize(edges)
    for (i <-i to num_iteration){
      val contribs = eRDD.join(rRDD).flatMap{
        case (s1,(s2,rank)) => s2.map(dest => (dest,rank/s2.size))
      }
//      rRDD = contribs.reduceByKey(_+_).mapValues(0.15+0.85*_)
    }
    val sorted_scores = rRDD.sortBy(_._2, false)
    val key_sentences = sorted_scores.take(top_k)
//    val key_sentences : Seq[Seq[String]] = sorted_scores.take(10).map(_._1).take(top_k)
    key_sentences.map(_._1)
  }

//  def textRank(sentences: Seq[Seq[String]]): Seq[Seq[String]] ={
//    val threshhold = 1
//    val top_k = 10
//    val i=0
//    var vertices: Array[(Long,Seq[String])]=Array()
//    var edges: Array[Edge[Double]] = Array()
//    for (i <- 0 until sentences.length){
//      val s1 = sentences(i)
//      vertices :+= (i.toLong,s1)
//      val j=0
//      for (j <- 0 until sentences.length){
//        if (i!=j){
//          val s2 = sentences(j)
//          val sim = cal_sen_similarity(s1,s2)
//          if (sim>threshhold){
//            edges :+= Edge(i.toLong,j.toLong,sim)
//            edges :+= Edge(j.toLong,i.toLong,sim)
//          }
//        }
//      }
//    }
//
//
//    val vRDD= sc.parallelize(vertices)
//    val eRDD= sc.parallelize(edges)
//    val graph = Graph(vRDD,eRDD)
//    val ranks = graph.pageRank(0.01).vertices
//    val scores = vRDD.join(ranks)
//    val sorted_scores = scores.sortBy(_._2._2, false)
//    val key_sentences = sorted_scores.take(10).map(_._2._1).take(top_k)
//    key_sentences
//  }

  def cal_sen_similarity(s1 : Seq[String],s2 : Seq[String]): Double ={
    val intersection = s1.intersect(s2)
    val sim = intersection.length/(math.log(s1.length)*math.log(s2.length))
    sim
  }
}
