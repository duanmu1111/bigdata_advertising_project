常用命令 Ctrl+P  参数提示

Alt+insert 导包

tools文件夹下
Bzip2Parquet: 将原始日志文件转换成parquet文件格式
Bzip2ParquetV2: 使用自定义类Log的方式构建schema信息，然后转换parquet文件格式

————————————————————————————————————————————————————————————————————————————————
report文件夹下

3.2.1地域分布相关的报表
ProCityRptV2: 利用sparkSql将查询的信息写到mysql，统计分组后的各省市的日志记录条数
ProCityRptV3: 利用spark算子将查询的信息保存到文件，统计分组后的各省市的日志记录条数

AreaAnalyseRpt:广告投放的地域分布统计，使用sparksql
AreaAnalyseRpt:广告投放的地域分布统计，使用类，初始化，很有趣，case class判断等，两种方式

媒体报表的分析
AppAnalyseRpt：媒体报表的分析，主要用到了broadcast
tools下的AppDict2Redis：媒体报表的分析，小表经常变化，可以使用redis，不用broadcast


商圈标签实现思路
1. 抽取日志中的精度和维度数据，并对经度维度数据进行过滤（留存中国范围内的经纬度数据）
2. 调用百度的API服务，返回经纬度对应的商圈数据     (web服务API -> 全球逆地理编码)
    2.1 注册百度lbs账号   LBS云
    2.2 在控制台创建一个应用ak, sk, sn
    
----> 商圈库

Tags4Ctx ---> 添加一个商圈标签的实现类 ---> 查询商圈库 ---> 商圈信息

httpClient
GeoHash
经纬度对应的Geohash