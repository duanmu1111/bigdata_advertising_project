package cn.dmp.tags

/*
 * @author  Duanmu sf
 * @date  2020/10/25 20:49
 * @Email:582836092@qq.com
 */

trait Tags {
    /**
      * 打标签的方法定义
      * @param args
      * @return
      */
    def makeTags(args: Any*) : Map[String, Int]

}
