package cn.dmp.utils

import org.apache.spark.sql.Row

/*
 * @author  Duanmu sf
 * @date  2020/10/25 20:45
 * @Email:582836092@qq.com
 */

object TagsUtils {
    val hasSomeUserIdConditition =
        """
          |imei!="" or imeimd5 != "" or imeisha1 != "" or
          |idfa!="" or idfaimd5 != "" or idfasha1 != "" or
          |mac!="" or macimd5 != "" or macsha1 != "" or
          |android!="" or androidimd5 != "" or androidsha1 != "" or
          |openudid!="" or openudidmd5 != "" or openudidsha1 != ""
                                    """.stripMargin


    def getAllUserId(v: Row): Any = {
        val userIds = new collection.mutable.ListBuffer[String]()


        if (v.getAs[String]("imei").nonEmpty)
            userIds.append("IM:" + v.getAs[String]("imei").toUpperCase)
        //........共15个
        if (v.getAs[String]("idfa").nonEmpty)
            userIds.append("IM:" + v.getAs[String]("idfa").toUpperCase)
        if (v.getAs[String]("mac").nonEmpty)
            userIds.append("IM:" + v.getAs[String]("mac").toUpperCase)
        if (v.getAs[String]("imei").nonEmpty)
            userIds.append("IM:" + v.getAs[String]("imei").toUpperCase)
        if (v.getAs[String]("imei").nonEmpty)
            userIds.append("IM:" + v.getAs[String]("imei").toUpperCase)
        if (v.getAs[String]("imei").nonEmpty)
            userIds.append("IM:" + v.getAs[String]("imei").toUpperCase)
        if (v.getAs[String]("imei").nonEmpty)
            userIds.append("IM:" + v.getAs[String]("imei").toUpperCase)
        if (v.getAs[String]("imei").nonEmpty)
            userIds.append("IM:" + v.getAs[String]("imei").toUpperCase)

        userIds

    }


}
