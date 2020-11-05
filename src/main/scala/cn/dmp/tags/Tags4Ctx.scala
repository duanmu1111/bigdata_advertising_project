package cn.dmp.tags

import cn.dmp.utils.TagsUtils
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName, util}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

/*
 * @author  Duanmu sf
 * @date  2020/10/25 19:46
 * @Email:582836092@qq.com
 */

object Tags4Ctx {
    def main(args: Array[String]): Unit = {
        if (args.length != 5) {
            println(
                """
                  |cn.dmp.report.Tags4Ctx
                  |参数：
                  | 输入路径
                  | 字典文件路径
                  | 停用词库
                  | 日期
                  | 输出路径
            """.stripMargin)
            sys.exit()
        }

        val Array(inputPath, dictFilePath, stopWordsFilePath, day,outputPath) = args


        // 2 创建sparkconf->sparkContext
        val sparkConf = new SparkConf()
        sparkConf.setAppName(s"${this.getClass.getSimpleName}")
        sparkConf.setMaster("local[*]")
        // RDD 序列化到磁盘 worker与worker之间的数据传输
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)
        val sQlContext = new SQLContext(sc)


        // 字典文件
        val dictMap = sc.textFile(dictFilePath).map(line => {
            val fields = line.split("\t", -1)
            (fields(4), fields(1))
        }).collect().toMap //只能在driver端进行广播，先收集到driver端

        // 字典文件
        val stopWordMap = sc.textFile(dictFilePath).map((_, 0)).collect().toMap //只能在driver端进行广播，先收集到driver端


        // 将字典数据广播executor
        val broadcastAppDict = sc.broadcast(dictMap)

        // 将停用词数据广播executor
        val broadcastStopWordsDict = sc.broadcast(stopWordMap)


        val load = ConfigFactory.load()
        
        
        //连接hbase
        //判断hbase中表是否存在，如果不存在则创建

         val hbTableName = load.getString("hbase.table.name")
        
        val configuration = sc.hadoopConfiguration
        configuration.set("hbase.zookeeper.quorum", load.getString("hbase.zookeeper.host"))
        val hbConn = ConnectionFactory.createConnection()
        val hbAdmin = hbConn.getAdmin
        
        if(hbAdmin.tableExists(TableName.valueOf(hbTableName))){
            println(s"$hbTableName 不存在....")
            println(s"正在创建 $hbTableName ...")

            val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbTableName))

            //创建列组
            val columnDescriptor = new HColumnDescriptor("cf")
            tableDescriptor.addFamily(columnDescriptor)
            
            //释放连接
            hbAdmin.close()
            hbConn.close()
        }


        //指定key的输出类型
        val jobConf = new JobConf(configuration)
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        //指定表的名称
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbTableName)
        
        
        //读取parquet文件
        sQlContext.read.parquet(inputPath).where(TagsUtils.hasSomeUserIdConditition).map(row => {
            //行数据进行标签化处理
            val ads = Tags4Ads.makeTags(row)
            val apps = Tags4App.makeTags(row, broadcastAppDict.value)
            val devices = Tags4Device.makeTags(row)
            val keywords = Tags4DKeyWord.makeTags(row, broadcastStopWordsDict.value)

            val allUserId = TagsUtils.getAllUserId(row)
            
            (allUserId(0), (ads ++ apps ++ devices ++ keywords).toList)
            
        }).reduceByKey((a,b) => {
            //List(("k电视剧"， 1),("APP爱奇艺"， 1),("k电视剧"， 1))
            
            //方式一
            (a++b).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList

            
            //方式二
            /*(a++b).groupBy(_._1).map{
                case (k, sameTags) => (k, sameTags.map(_._2).sum)
            }.toList*/
            
            
            
        }).map{
            case (userId, userTags) => {
                val put = new Put(Bytes.toBytes(userId))
                val tags = userTags.map(t => t._1 + ":" + t._2).mkString(",")
                
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(s"day$day"), Bytes.toBytes(tags))
                
                (new ImmutableBytesWritable(), put)   //ImmutableBytesWritable => rowkey
                
                
            }
        }.saveAsHadoopDataset(jobConf)
          //saveAsTextFile(outputPath)
        
        sc.stop()

        
        
        





    }

}
