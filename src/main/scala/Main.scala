import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, PriorityQueue}

object Main {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HierarchicalClustering").setMaster("local[*]")
    val sc = new SparkContext(conf)

var counter=0
    case class Flower(sepalLength: Double,sepalWidth: Double,petalLength: Double,petalWidth: Double,flowerClass:String,ID:Long)
    case class Cluster(centroid:Flower,clusterMembers:ListBuffer[Flower])
    case class Distance_Cluster1_2(distance:Double,cluster1:Cluster,cluster2:Cluster)



//    val data = sc.textFile(args(0)).filter(x=>x.isEmpty==false)
    val data = sc.textFile("input/iris.data").filter(x=>x.isEmpty==false)
    val allFlowers=data.map(line => line.split(",")).zipWithIndex
      .map(line=> Cluster(Flower(line._1(0).toDouble, line._1(1).toDouble, line._1(2).toDouble, line._1(3).toDouble,"Centroid",1.toLong),ListBuffer(Flower(line._1(0).toDouble, line._1(1).toDouble, line._1(2).toDouble, line._1(3).toDouble,line._1(4),line._2)))).collect()

//    val clustersK=args(1).toInt
    val clustersK=3




    def heapOrder(d: Distance_Cluster1_2) = d.distance


    var minHeap = new PriorityQueue[Distance_Cluster1_2]()(Ordering.by(heapOrder).reverse)

    def clusterDistance(c1:Flower,c2:Flower): Double =
    {
      var sigma=0.0
      sigma+=math.pow((c1.petalLength-c2.petalLength),2)
      sigma+=math.pow((c1.petalWidth-c2.petalWidth),2)
      sigma+=math.pow((c1.sepalLength-c2.sepalLength),2)
      sigma+=math.pow((c1.sepalWidth-c2.sepalWidth),2)


      return math.sqrt(sigma)

    }

    for(flower<-allFlowers)
    {
      for(otherFlower<-allFlowers)
      {
        if(flower!=otherFlower)
        {
          val dist=clusterDistance(flower.centroid,otherFlower.centroid)
          minHeap.enqueue(Distance_Cluster1_2(dist,flower,otherFlower))
        }
      }

    }


    def createNewCentroid(newCluster:List[Flower]): Flower =
    {
      var centroidSepalLength=0.0
      var centroidSepalWidth=0.0
      var centroidPetalLength=0.0
      var centroidPetalWidth=0.0


      for (flower <- newCluster) {
        centroidSepalLength=centroidSepalLength+flower.sepalLength
        centroidSepalWidth=centroidSepalWidth+flower.sepalWidth
        centroidPetalLength=centroidPetalLength+flower.petalLength
        centroidPetalWidth=centroidPetalWidth+flower.petalWidth
      }
      centroidSepalLength=centroidSepalLength/newCluster.size
      centroidSepalWidth=centroidSepalWidth/newCluster.size
      centroidPetalLength=centroidPetalLength/newCluster.size
      centroidPetalWidth=centroidPetalWidth/newCluster.size

      return Flower(centroidSepalLength,centroidSepalWidth,centroidPetalLength,centroidPetalWidth,"Centroid",1.toLong)
    }


    def mergeClusters(C1: Cluster, C2: Cluster): Cluster =
    {
      val newCluster=C1.clusterMembers++C2.clusterMembers
      return Cluster(createNewCentroid(newCluster.toList),newCluster)
    }


    var noOfClustersFormed=allFlowers.size
    var finalClusters=Set[Cluster]()
    var alreadyMergedClusters=Set[Cluster]()

    while(noOfClustersFormed>clustersK)
    {
      var currentClusters=Set[Cluster]()

      val dist_C1_C2=minHeap.dequeue()
      val C1=dist_C1_C2.cluster1
      val C2=dist_C1_C2.cluster2

      alreadyMergedClusters+=C1
      alreadyMergedClusters+=C2

      currentClusters+=mergeClusters(C1,C2)

      while(!minHeap.isEmpty)
      {
        val dist_C1_C2=minHeap.dequeue()
        val C1=dist_C1_C2.cluster1
        val C2=dist_C1_C2.cluster2
        if(!alreadyMergedClusters.contains(C1))
        {
          currentClusters+=C1
        }
        if(!alreadyMergedClusters.contains(C2))
        {
          currentClusters+=C2
        }
      }



      for (newCluster <- currentClusters)
      {
        for (newClusterAgain <- currentClusters)
        {
          if(newCluster!=newClusterAgain)
          {
            val dist=clusterDistance(newCluster.centroid,newClusterAgain.centroid)
            minHeap.enqueue(Distance_Cluster1_2(dist,newCluster,newClusterAgain))
          }
        }

      }


      noOfClustersFormed=currentClusters.size
      if(noOfClustersFormed==clustersK)
      {
        finalClusters=currentClusters
      }

    }

    var finalClusterName_clusterMembers=mutable.Set[(String,List[Flower])]()

    for (cluster <- finalClusters)
    {
      var clusterNames_count=mutable.HashMap[String,Int]()
      for (clusterNode <- cluster.clusterMembers)
      {
        if(clusterNames_count.contains(clusterNode.flowerClass))
        {
          clusterNames_count(clusterNode.flowerClass)=clusterNames_count(clusterNode.flowerClass)+1
        }
        else
        {
          clusterNames_count.put(clusterNode.flowerClass,1)
        }

      }
      var classNameCount=0
      var className=""
      clusterNames_count.foreach(x=>if(x._2>=classNameCount){className=x._1;classNameCount=x._2})
      finalClusterName_clusterMembers.add(className,cluster.clusterMembers.toList)
    }

    new File("Clusters.txt" ).delete()
    val pw = new PrintWriter(new File("Clusters.txt" ))
    var count = 0
    for(elem<-finalClusterName_clusterMembers){
      pw.println("cluster:"+elem._1)
      for(x<-elem._2){
        pw.println("["+x.sepalLength+", "+x.sepalWidth+", "+x.petalLength+", "+x.petalWidth+", "+"'"+x.flowerClass+"'"+"]")
        if(elem._1!=x.flowerClass){
          count=count+1
        }
      }
      pw.println("No of points in the cluster:"+elem._2.size+"\n")

    }
    pw.println("No of points wrongly assigned:"+count)



    pw.close()
  }

}

