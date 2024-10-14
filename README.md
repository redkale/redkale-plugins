## 项目介绍
&emsp;&emsp;RedKale的扩展插件库。

## 版本
```xml
<dependency>
    <groupId>org.redkalex</groupId>
    <artifactId>redkale-plugins</artifactId>
    <version>2.8.0</version>
</dependency>
```

## 功能说明
|组件|说明|依赖|
| --- | --- | --- |
|RedisCacheSource|基于redkale-client的CacheSource实现|无|
|RedisVertxCacheSource|基于vertx-redis的CacheSource实现|vertx-redis-client:v4.5.8|
|RedissonCacheSource|基于redisson的CacheSource实现|redisson:v3.31.0|
|  |  |
|VertxSqlDataSource|基于vertx-sql-client的DataSource实现|vertx-sql-client:v4.5.8|
|MongodbDriverDataSource|基于mongodb的DataSource实现|mongodb-driver-reactivestreams:v5.1.0|
|OpenSearchSource|基于openSearch的SearchSource实现|无|
|  |  |
|ConsulClusterAgent|基于consul的ClusterAgent实现|无|
|NacosClusterAgent|基于nacos的ClusterAgent实现|无|
|  |  |
|ApolloClientPropertiesAgent|基于apollo-client的PropertiesAgent实现|apollo-client:v2.2.0
|ApolloPropertiesAgent|基于apollo的PropertiesAgent实现|无|
|NacosClientPropertiesAgent|基于nacos-client的PropertiesAgent实现|nacos-client:v2.2.4
|NacosPropertiesAgent|基于nacos的PropertiesAgent实现|无|
|  |  |
|XxljobScheduledManager|基于xxl-job的ScheduledManager实现|无|
|  |  |
|DataNativeJsqlParser|基于jsqlparser的DataNativeSqlParser实现|jsqlparser:v4.10|
|  |  |
|KafkaMessageAgent|基于kafka的MessageAgent实现|kafka-clients:v3.7.0|


<b>详情请访问:&nbsp;&nbsp;&nbsp;&nbsp;<a href='http://redkale.org' target='_blank'>http://redkale.org</a></b>
