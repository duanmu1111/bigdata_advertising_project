package cn.dmp.tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
/*
 * @author  Duanmu sf
 * @date  2020/10/25 20:51
 * @Email:582836092@qq.com
 */

object Tags4Ads extends Tags {
    /**
      * 打标签的方法定义
      *
      * @param args
      * @return
      */
    override def makeTags(args: Any*): Map[String, Int] = {
        val row = args(0).asInstanceOf[Row]
        
        var map = Map[String, Int]()
        //广告位类型和名称
        val adTypeId = row.getAs[Int]("adspacetype")
        val adTypeName = row.getAs[String]("adspacetypename")
        
        if(adTypeId > 9) map += "LC"+adTypeId->1
        else if(adTypeId>0) map += "LC0" +adTypeId -> 1
        
        if(StringUtils.isNotEmpty(adTypeName)) map += "LN" +adTypeName ->1


        val chanelId = row.getAs[Int]("adplatformproviderid")
        if(chanelId > 0) map += (("CN" +chanelId, 1))
        
        map
    }
}
