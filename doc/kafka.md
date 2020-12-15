讲解kafka的what、why、how，最后讲一下rabbitmq，mq技术选型和横向对比。

### Kafka是啥？

kafka是一个消息队列，是 一个消息中间件。

**引入队列的作用：**

没有队列，一个service调用另一个service，两者强依赖。一方修改另一方可能需要跟着修改。

![1607235614293](D:\Z_lhy\STUDY\Kafka\doc\img\1607235614293.png)

**引入消息系统**

在service1和service2之间引入一个中间件消息系统，service1将消息往中间件里推，然后继续自己的其他处理。

service2拉取中间件中的消息进行处理，这样可以做到 **解耦、削峰、异步**。中间件有很多比如存储的，缓存的，消息的，现在是面向消息的。最终都是和分布式相关的，分布式关于中11111111111间件有几个词汇，必须是：**可靠的、开扩展、高性能**。

![1607238822304](D:\Z_lhy\STUDY\Kafka\doc\img\1607238822304.png)

上图的消息中间件存在的问题就是：1单点问题，2性能问题

**解决性能问题：**

### Kafka中的AKF

如果左边所有业务系统的消息都打到MSG消息系统，右边虽然能够接收，但是不能“分而治之”，从AKF的角度

![1607241363776](D:\Z_lhy\STUDY\Kafka\doc\img\1607241363776.png)

**X轴：**高可用。解决单点问题

**Y轴：**按业务划分，不同业务的消息汇聚到一起。产生业务的隔离，不同业务可以部署到不同的节点上，每个节点只关注自己的东西。（kafka里就是topic），比如日志的消息，埋点数据的消息。

**Z轴：**数据分片，分区（kafka里就是partition ）

#### Kafka中的Y轴

在fakfa里，MSG系统在逻辑层先拆，第一出现的词汇就是**Topic**，是一个逻辑概念，虚的（业务，AKF的Y轴）

![1607243566049](D:\Z_lhy\STUDY\Kafka\doc\img\1607243566049.png)

单看某一个业务的topic，比如日志的消息量太大，如果是单看一个进程的话，工作起来性能不是很强，IO上会卡顿，根据AKF再拆分的话，就是拆分区了。在topic里还有个概念就是partition 。

#### Kafka中的Z轴

<img src="D:\Z_lhy\STUDY\Kafka\doc\img\1607244009442.png" alt="1607244080547" style="zoom:67%;" />

partition在AKF里就是Z轴了。

比如将Y轴日志binlog，由一个变为多个，z轴有多个分取，由多台机器去承载它；

埋点的消息，也会有埋点的一套partition分区。

<img src="C:\Users\lihaoyang6\AppData\Roaming\Typora\typora-user-images\1607243741916.png" alt="1607243741916" style="zoom: 67%;" />

​	

Z轴是对Y轴的一个东西的细分，对一个东西进行细分的手段有很多：range、随机、hash、做一个映射。

**生产和消费顺序的一致性**

如果Y轴是mysql的binlog（具有顺序性的业务场景），本来Y轴就**一个**binlog日志，我生产者直接放到一个队列里头，消费者消费的时候直接拿走就行了。现在要把这个binlog打散到多台机器，放在不同的分区，该怎么打才合适？怎么才能保证消费的顺序一致性？数据从一个地方散列到多个地方的时候，怎么才能保证生产和消费消息顺序的一致性？

没有分区的时候，数据直接推进topic什么样子，消费方取出的还是什么样子。

![1607246546338](D:\Z_lhy\STUDY\Kafka\doc\img\1607246546338.png)

加了分区，

<img src="D:\Z_lhy\STUDY\Kafka\doc\img\1607246961900.png" alt="1607246961900" style="zoom:80%;" />

分而治之是好的，但是也会带来坏处，会打乱顺序。所以**一个原则：**无关的分而治之，有关的一定要保证顺序。

比如生产方产生mysq的binlog，binlog里有A表的日志，有B表的日志，有C表的日志，可能是先修改A表再修改B表，然后把binlog扔到Kafka，在消费者这一方，我可以先修改B表，再修改A表只要保证A、B表各自的修改保证顺序

（图中所示的，update2 B表一定要在update1B表后面，update2 A表一定要在update1 A表后面，每个表数据的一致性，按生产的者的顺序，提交事务），这就是有关的数据一定要保证顺序。



![](D:\Z_lhy\STUDY\Kafka\doc\img\1607843315485.png)

在Kafka中，单机的性能低，就需要多机，每个机器里有自己的partition，生产者向Kafka推送数据的时候，要规划好数据路由，把无关的数据扔到不同的分区。不这样做的话，可能要在消费方用分布式锁来保证顺序从而不能够并行处理。比如：把商品1的数据扔到partition1，商品2的数据扔到partition2；表1的数据扔到partition1，表2的数据扔到partition2。

**结论：**在同一个topic里

无关的数据分散在不同的分区里，以追求并发并行；

有关的数据，一定要按原有顺序发送到同一个分区里；

#### Kafka中的X轴

不同的业务消息分散到不同的分区了，可以并行地提升性能。

<img src="D:\Z_lhy\STUDY\Kafka\doc\img\1607846204787.png" alt="1607846204787" style="zoom:67%;" />

但是如果某一个分区挂掉了，丢失了，会造成消息的丢失。一般会做一个副本。为了X轴，副本的概念，是出主机的，如果在一台机器上则意义不大。可以有读写分离，但是容易出现一致性问题，Kafka做了降维，只能在主的分片上进行读写。

```
从单机到分布式，分布式中间件与AKF之间是由必然关系的。redis、es
```

在一个partition分区的内部是有序的，各个分区之间是无序的，消费者按照一个分区读取，读到的就是推进去的顺序，有点像纯队列。读取到的位置叫**偏移量offest**

![1607849891913](D:\Z_lhy\STUDY\Kafka\doc\img\1607849891913.png)

Zookeeper

在Kafka的架构图中，会有zookeeper集群的出现。企业中使用的时候，会有很多的topic，很多的partition，很多的主从复制。如何去管理这些分布式的东西？在经验中，单机管理、主从集群的管理是成本最低的。大部分的集群如zookeeper对外提供服务的时候也是一个leader，一些follower。这时候就会牵扯到主从的选主过程、以及元数据的管理问题。只要由主从集群，就会由分布式协调的问题。分布式协调的代表作就是zookeeper和etcd。

通过上边推导的逻辑的拓扑的时候，你就知道它未来一定是牵扯到分布式的，多机的，非常复杂的环节，依赖zookeeper，就是依赖他的分布式协调，zookeeper就是一个协调者，千万不要用于storage存储。

### Broker

概念：

controller

admin

#### producer：

向分区填充数据。

​	老版本的中，producer也从zk拿数据，找到zk，从zk获得所有的broker的注册信息

![1607855152828](D:\Z_lhy\STUDY\Kafka\doc\img\1607855152828.png)

​	新版本中，producer不是从zk中拿，需要手动给出broker的列表（ip地址），它随便找一个连接，就可以从一个连接中获取到所有的broker的通讯地址。

**问题：问什么新版本不从zookeeper中拿broker的信息？**

zookeeper就是一个分布式的协调者，不应该拿他当一个storage，·它是主从的，所有的增删改都压到了leader身上，所以不应该对它有过多的访问，（所有的broker 会注册到zookeeper，一些mate元数据也会放在zookeeper）不能说你集群的状态的信息变更，都咣咣咣的去访问zk，公司有很多部门，几百几千个连接，咣咣咣都去访问zookeeper，可能回吧zookeeper的leader的网卡打满，会有负担。所以会把一些核心的，致命性的问题放到zookeeper，必须选主，就到zookeeper了。曾经broker的状态、分区等依赖于zookeeper的一些元数据的管理要迁移出来。

<img src="D:\Z_lhy\STUDY\Kafka\doc\img\1607855774414.png" alt="1607855774414" style="zoom:50%;" />

有了controller之后，慢慢的把一些功能切出来了，controller把一些元数据信息：比如broker的注册列表，topic、partition等跟客户端（producer、consumer）无关的集群内部的一些东西，在集群内部进行同步，在业务层次集群间就可以通信，而不依赖zookeeper。

```properties
总之就是：在业务层次上，分布式角色(broker、producer、consumer、topic...)之间就可以通信，不要因为业务需求，让ZK集群成为负担。
```

![1607922402203](D:\Z_lhy\STUDY\Kafka\doc\img\1607922402203.png)

在并发下，注意一致性的问题，多个producer分布在不同的tomcat里，如果需要数据的有序性，需要加分布式锁来保证一致性。

```java
lock{
    sql操作;
    producer-推送到-kafka;推送数据一定要在锁里。不能写在unlock里
}unlock{}
```

**相关的数据放在一个分区里**





broker、topic、partition关系图



Broker是Kafka的概念，是一个物理进程。

1，所有的broker 会注册到zookeeper，一些mate元数据也会放在zookeeper，用zookeeper选出一个controller（老大），由管理员准备topic，然后划分分区的映射关系，



#### Consumer

**一个Consumer**

producer有很多，不断产生大量消息到消息队列，消息队列把数据保持到不同的分区，能hold住这些IO的流量，把消息堆进来，consumer只有一个也可以。等于是对接了不同吞吐量的系统，producer那边是异步的，消息打进消息队列就算完成业务了，consumer慢慢地消费消息也可以。

一个Consumer可以消费多个分区，可以保证每个分区消息的顺序性（每个分区是不同的业务消息，保证每个分区的顺序性无可非议）。

<img src="D:\Z_lhy\STUDY\Kafka\doc\img\1607993530905.png" alt="1607993530905" style="zoom:67%;" />

**多个Consumer**

如果追求consumer的性能,快速消费消息，需要多个consumer，多个consumer怎么消费partition？

最好的情况：

**consumer-partition一一对应：**如下图consumer-1消费partition-0分区，consumer-2消费partition-1分区。





<img src="D:\Z_lhy\STUDY\Kafka\doc\img\1607993817931.png" alt="1607993817931" style="zoom:67%;" />

```
从上面可以看出，分区和consumer的关系可以是：
partition：consumer
	N    :    1
	1    :    1
	1    :    N  绝对不允许，破坏了有序性
```









