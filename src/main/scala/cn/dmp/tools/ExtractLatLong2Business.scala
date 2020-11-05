package cn.dmp.tools

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @author  Duanmu sf
 * @date  2020/11/4 21:38
 * @Email:582836092@qq.com
 */
/**
  * 抽取日志字段中的经纬度，并请求百度的api获取商圈信息
  * 
  */
object ExtractLatLong2Business {
    def main(args: Array[String]): Unit = {


        // 0 校验参数个数
        if (args.length != 1) {
            println(
                """
                  |cn.dmp.tools.AppDict2Redis
                  |参数：
                  | appdictInputPath
                """.stripMargin)
            sys.exit()
        }
        // 1 接受程序参数
        val Array(inputPath) = args

        // 2 创建sparkconf->sparkContext
        val sparkConf = new SparkConf()
        sparkConf.setAppName(s"${this.getClass.getSimpleName}")
        sparkConf.setMaster("local[*]")
        // RDD 序列化到磁盘 worker与worker之间的数据传输
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)
        val sQLContext = new SQLContext(sc)
        
        sQLContext.read.parquet(inputPath)   //lat为经度 long为纬度, inputPath为一个包含经度纬度的文件
          .select("lat", "long")
          .where("lat > 3 and lat < 54 and long > 73 and long < 136" )
          .distinct()
          .map(row => {
              val lat = row.getAs[String]("lat")
              val Longs = row.getAs[String]("long")
              
              GeoHash.w
              
              
              
          })
         
    }

}
