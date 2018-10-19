import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object getKeySentences {

  val conf = new SparkConf().setAppName("get_key_sen").setMaster("local");
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().master("local").appName("sds")
    .config("spark.some.config.option", "some-value").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val i = 0;
    for( i <- 0 to 8) {
      //      val wordFile = s"file:///C:/Users/31476/Desktop/543/bytecup2018/bytecup.corpus.train.${i}.txt"
      val wordFile = s"file:////home/wsy/桌面/Bytecup2018/preprocess_data/processed_train.${i}.txt"
      var data = readContent(wordFile)
//      data.printSchema()
      data.map(row => {
        val row1 = row.getAs[List[List[String]]]("content")
        val make = textRank(row1)
        (make,row(1),row(2)) })
      print(1)

      //      val key_sentences = textRank(sentences)
      //      val newdf = data.withColumn("content", when(col("content")===sentences, key_sentences).otherwise(col("content")))

      //      for(i <- 0 until data.count()){
      //        val row = data.iloc[i]
      //        var text = raw(i).toString()
      //        text = text.substring(1,text.length()-1)
      //        val key_sentences = textRank(text)
      //        var sen_text = key_sentences(0)
      //        var m = 1
      //        while (m < key_sentences.length) {
      //          sen_text = sen_text+"."+key_sentences(i)
      //          i += 1
      //        }
      //        val newdf = data.withColumn("content",when(col("content")===text, sen_text).otherwise(col("content")))
      //        data = newdf
      //        print(1)
      //      }

    }
  }

  def readContent(path : String) ={
    val spark = SparkSession.builder().master("local").appName("readWords")
      .config("spark.some.config.option", "some-value").getOrCreate()
    import spark.implicits._
    val stringFrame = spark.read.text(path).as[String]
    val jsonFrame = spark.read.json(stringFrame)
    //    val data = jsonFrame.select("id","title", "content")
    //    data
    jsonFrame
  }

  def textRank(sentences: List[List[String]]): Array[List[String]] ={
    val threshhold = 0
    val top_k = 10
    val i=0
    var vertices: Array[(Long,List[String])]=Array()
    var edges: Array[Edge[Double]] = Array()
    for (i <- 0 until sentences.length){
      val s1 = sentences(i)
      vertices :+= (i.toLong,s1)
      val j=0
      for (j <- 0 until sentences.length){
        if (i!=j){
          val s2 = sentences(j)
          val sim = cal_sen_similarity(s1,s2)
          if (sim>threshhold){
            edges :+= Edge(i.toLong,j.toLong,sim)
            edges :+= Edge(j.toLong,i.toLong,sim)
          }
        }
      }
    }
    val vRDD= sc.parallelize(vertices)
    val eRDD= sc.parallelize(edges)
    val graph = Graph(vRDD,eRDD)
    val ranks = graph.pageRank(0.0001).vertices
    val scores = vRDD.join(ranks)
    val sorted_scores = scores.sortBy(_._2._2, false)
    val key_sentences = sorted_scores.take(10).map(_._2._1).take(top_k)
    key_sentences
  }

  def cal_sen_similarity(s1 : List[String],s2 : List[String]): Double ={
    val intersection = sc.parallelize(s1).intersection(sc.parallelize(s2))
    val sim = intersection.count()/(math.log(s1.length)*math.log(s2.length))
    sim
  }

}
