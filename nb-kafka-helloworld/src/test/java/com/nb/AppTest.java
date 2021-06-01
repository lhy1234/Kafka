package com.nb;

import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;


import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class AppTest {

    @Test
    public void producer() throws Exception {

        Properties props = new Properties();

        //host配了node01、node02、node03和虚拟机ip映射了
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");
        //kafka是持久化数据的MQ，数据是byte[],不会对数据进行干预，双方需要约定编解码,生产者编码
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //ACK
        props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        // props.setProperty(ProducerConfig.CLIENT_ID_CONFIG,"nb-items-producer-1");
        //kafka使用零拷贝，sendfile 系统调用实现快速数据消费
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        /**
         * 已经使用命令创建了个topic : nb-items 2个分区
         *  //同一类商品会被发到同一个分区
         */
        String topic = "msb-items";
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                //消息对象~对消息的封装，业务的消息就是value属性
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "item-" + j, "val-" + i);

                Future<RecordMetadata> send = producer.send(record);

                RecordMetadata rm = send.get();
                int partition = rm.partition();
                long offset = rm.offset();

                System.out.println("key :" + record.key() + " value :" + record.value() + " partition: " + partition + " offset:" + offset);
            }
        }
    }


    @Test
    public void consumerCommitByHand() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");

        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "NB-01");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//自动异步提交

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("msb-items"));

        while (true) {
            /**
             * 常识：如果多线程处理多个分区
             * 每poll一次，就是一个job启动
             * 一个job用多线程并行处理分区，
             * 且job应该被控制是串行的，也就是poll是串行的，
             * 否则一个poll内的数据没有处理完没有更新offset，再poll就会重复消费
             */
            // 每次poll到的是多个分区的消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));
            if (!records.isEmpty()) {
                System.out.println("-------------拉取了 " + records.count() + " 条数据------------------");
                //拿到所分区
                Set<TopicPartition> partitions = records.partitions();


                /**
                 * 1.每处理一条记录后提交
                 * 2.分区处理完后分区提交
                 * 3.poll回的一批处理完后，按批次提交
                 */
                for (TopicPartition tp : partitions) {
                    //按分区拿到该分区的消息,相当于按分区分组了
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);

                    //线性处理分区消息，或者每个分区起多线程处理
                    partitionRecords.forEach(record -> {
                        int partition = record.partition();
                        long offset = record.offset();
                        String key = record.key();
                        String value = record.value();
                        System.out.println("key :" + key + " value :" + value + " partition: " + partition + " offset:" + offset);


//                        Map<TopicPartition,OffsetAndMetadata> offsetMap = new HashMap<>();
//                        TopicPartition topicPart = new TopicPartition("nb-items",partition);
//                        OffsetAndMetadata om = new OffsetAndMetadata(offset);
//                        offsetMap.put(topicPart,om);
//                        //1.最安全，每条记录一提交
//                        consumer.commitSync(offsetMap);
                    });

                    //2.分区粒度提交
                    //获取本分区最后一条记录的offset
                    long pLastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    OffsetAndMetadata om = new OffsetAndMetadata(pLastOffset);
                    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
                    offsetMap.put(tp, om);
                    consumer.commitSync();
                }

                //3.写在这是按批次提交offset
                //consumer.commitAsync();
            }
        }
    }


    @Test
    public void consumerOne2Many() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");
        //消费者解码
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //设置group
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OOXX04");
        //earliest：从头拉取
        //latest(默认)：从分区末尾开始拉取
        //none：找不到offset时抛异常
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");//自动异步提交，容易丢数据&&重复消费
        //props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"10000");//默认5秒
        //props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"3");//poll拉取数据，弹性按需拉取多少？

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("msb-items"));

        while (true) {

            //阻塞等待拉取消息，
            // 每次poll到的是多个分区的消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));
            if (!records.isEmpty()) {
                System.out.println("-------------拉取了 " + records.count() + " 条数据------------------");
                //拿到所分区
                Set<TopicPartition> partitions = records.partitions();
                for (TopicPartition tp : partitions) {
                    //按分区拿到该分区的消息,相当于按分区分组了
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);
                    //线性处理分区消息，或者每个分区起多线程处理
                    partitionRecords.forEach(record -> {
                        int partition = record.partition();
                        long offset = record.offset();
                        String key = record.key();
                        String value = record.value();
                        System.out.println("key :" + key + " value :" + value + " partition: " + partition + " offset:" + offset);
                    });
                }
            }

        }
    }

    @Test
    public void consumer() {
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");
        //消费者解码
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //设置group
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OOXX02");

        /**
         * auto.offset.reset
         * 当消费者找不到记录的offset时（新建立的消费者组），根据此配置标记从何处开始消费
         * earliest：从头0消费
         * latest(默认)：从分区末尾开始消费
         * none：找不到offset时抛异常
         */
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");//自动异步提交，容易丢数据&&重复消费
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");//默认5秒
        //props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"3");//poll拉取数据，弹性按需拉取多少？

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("msb-items"));

        while (true) {
            //阻塞等待拉取消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));
            if (records.count() != 0) {
                System.out.println("-------------拉取了 " + records.count() + " 条数据------------------");
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    int partition = record.partition();
                    long offset = record.offset();

                    System.out.println("key :" + record.key() + " value :" + record.value() + " partition: " + partition + " offset:" + offset);
                }
            }

        }
    }


    /**
     * 消费者负动态载均衡
     */
    @Test
    public void consumerBalance() {
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //设置group
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OOXX03");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");//自动异步提交，容易丢数据&&重复消费
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //kafka的consumer会动态负载均衡
        consumer.subscribe(Arrays.asList("nb-items"), new ConsumerRebalanceListener() {
            //撤销的分区
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.err.println("---进入撤销分区方法----");
                for (TopicPartition partition : partitions) {
                    System.err.println("撤销了partition-" + partition.partition());
                }
            }

            //分配的分区
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.err.println("----进入分配分区方法----");
                for (TopicPartition partition : partitions) {
                    System.err.println("分配了partition-" + partition.partition());
                }
            }
        });

        while (true) {
            //阻塞等待拉取消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));
            if (records.count() != 0) {
                System.out.println("-------------拉取了 " + records.count() + " 条数据------------------");
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    int partition = record.partition();
                    long offset = record.offset();
                    System.out.println("key :" + record.key() + " value :" + record.value() + " partition: " + partition + " offset:" + offset);
                }
            }

        }
    }


    @Test
    public void consumerByTimestamp() {
        //基础配置
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node01:9092");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //消费的细节
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OOXX");
        //KAKFA IS MQ  IS STORAGE
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//第一次启动，米有offset
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");//自动提交时异步提交，丢数据&&重复数据
        //一个运行的consumer ，那么自己会维护自己消费进度
        //一旦你自动提交，但是是异步的
        //1，还没到时间，挂了，没提交，重起一个consuemr，参照offset的时候，会重复消费
        //2，一个批次的数据还没写数据库成功，但是这个批次的offset背异步提交了，挂了，重起一个consuemr，参照offset的时候，会丢失消费

//        p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"15000");//5秒
//        p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,""); // POLL 拉取数据，弹性，按需，拉取多少？
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);


        /**
         * 以下代码是你再未来开发的时候，向通过自定时间点的方式，自定义消费数据位置
         *
         * 其实本质，核心知识是seek方法
         *
         * 举一反三：
         * 1，通过时间换算出offset，再通过seek来自定义偏移
         * 2，如果你自己维护offset持久化~！！！通过seek完成
         *
         */

        Map<TopicPartition, Long> tts = new HashMap<>();
        //通过consumer取回自己分配的分区 as

        Set<TopicPartition> as = consumer.assignment();

        while (as.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            as = consumer.assignment();
        }

        //自己填充一个hashmap，为每个分区设置对应的时间戳
        for (TopicPartition partition : as) {
//            tts.put(partition,System.currentTimeMillis()-1*1000);
            tts.put(partition, 1610629127300L);
        }
        //通过consumer的api，取回timeindex的数据
        Map<TopicPartition, OffsetAndTimestamp> offtime = consumer.offsetsForTimes(tts);


        for (TopicPartition partition : as) {
            //通过取回的offset数据，通过consumer的seek方法，修正自己的消费偏移
            OffsetAndTimestamp offsetAndTimestamp = offtime.get(partition);
            long offset = offsetAndTimestamp.offset();  //不是通过time 换 offset，如果是从mysql读取回来，其本质是一样的
            System.out.println(offset);
            consumer.seek(partition, offset);

        }

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }


        while (true) {
            /**
             * 常识：如果想多线程处理多分区
             * 每poll一次，用一个语义：一个job启动
             * 一次job用多线程并行处理分区
             * 且，job应该被控制是串行的
             * 以上的知识点，其实如果你学过大数据
             */
            //微批的感觉
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));// 0~n

            if (!records.isEmpty()) {
                //以下代码的优化很重要
                System.out.println("-----------" + records.count() + "-------------");
                Set<TopicPartition> partitions = records.partitions(); //每次poll的时候是取多个分区的数据
                //且每个分区内的数据是有序的

                /**
                 * 如果手动提交offset
                 * 1，按消息进度同步提交
                 * 2，按分区粒度同步提交
                 * 3，按当前poll的批次同步提交
                 *
                 * 思考：如果在多个线程下
                 * 1，以上1，3的方式不用多线程
                 * 2，以上2的方式最容易想到多线程方式处理，有没有问题？
                 */
                for (TopicPartition partition : partitions) {
                    List<ConsumerRecord<String, String>> pRecords = records.records(partition);
//                    pRecords.stream().sorted()
                    //在一个微批里，按分区获取poll回来的数据
                    //线性按分区处理，还可以并行按分区处理用多线程的方式
                    Iterator<ConsumerRecord<String, String>> piter = pRecords.iterator();
                    while (piter.hasNext()) {
                        ConsumerRecord<String, String> next = piter.next();
                        int par = next.partition();
                        long offset = next.offset();
                        String key = next.key();
                        String value = next.value();
                        long timestamp = next.timestamp();


                        System.out.println("key: " + key + " val: " + value + " partition: " + par + " offset: " + offset + "time:: " + timestamp);

                        TopicPartition sp = new TopicPartition("msb-items", par);
                        OffsetAndMetadata om = new OffsetAndMetadata(offset);
                        HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                        map.put(sp, om);

                        consumer.commitSync(map);//这个是最安全的，每条记录级的更新，第一点
                        //单线程，多线程，都可以
                    }
                    long poff = pRecords.get(pRecords.size() - 1).offset();//获取分区内最后一条消息的offset


                    OffsetAndMetadata pom = new OffsetAndMetadata(poff);
                    HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    map.put(partition, pom);
                    consumer.commitSync(map);//这个是第二种，分区粒度提交offset
                    /**
                     * 因为你都分区了
                     * 拿到了分区的数据集
                     * 期望的是先对数据整体加工
                     * 小问题会出现？  你怎么知道最后一条小的offset？！！！！
                     * 感觉一定要有，kafka，很傻，你拿走了多少，我不关心，你告诉我你正确的最后一个小的offset
                     */

                }
                consumer.commitSync();//这个就是按poll的批次提交offset，第3点


//                Iterator<ConsumerRecord<String, String>> iter = records.iterator();
//                while(iter.hasNext()){
//                    //因为一个consuemr可以消费多个分区，但是一个分区只能给一个组里的一个consuemr消费
//                    ConsumerRecord<String, String> record = iter.next();
//                    int partition = record.partition();
//                    long offset = record.offset();
//                    String key = record.key();
//                    String value = record.value();
//
//                    System.out.println("key: "+ record.key()+" val: "+ record.value()+ " partition: "+partition + " offset: "+ offset);
//                }
            }


        }

    }

    @Test
    public void consumerByTime() {
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");
        //消费者解码
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //设置group
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OOXX");


        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");//自动异步提交，容易丢数据&&重复消费
        //props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");//默认5秒
        //props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"3");//poll拉取数据，弹性按需拉取多少？

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("msb-items"));
        /**
         * 以下代码是你再未来开发的时候，向通过自定时间点的方式，自定义消费数据位置
         *
         * 其实本质，核心知识是seek方法
         *
         * 举一反三：
         * 1，通过时间换算出offset，再通过seek来自定义偏移
         * 2，如果你自己维护offset持久化~！！！通过seek完成
         *
         */

        Map<TopicPartition, Long> tts = new HashMap<>();
        //通过consumer取回自己分配的分区 as

        Set<TopicPartition> as = consumer.assignment();

        while (as.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            as = consumer.assignment();
        }

        //自己填充一个hashmap，为每个分区设置对应的时间戳
        for (TopicPartition partition : as) {
            tts.put(partition,System.currentTimeMillis()-60*1000*1);
            //tts.put(partition, 1610629127300L);
        }
        //通过consumer的api，取回timeindex的数据
        Map<TopicPartition, OffsetAndTimestamp> offtime = consumer.offsetsForTimes(tts);


        for (TopicPartition partition : as) {
            //通过取回的offset数据，通过consumer的seek方法，修正自己的消费偏移
            OffsetAndTimestamp offsetAndTimestamp = offtime.get(partition);
            long offset = offsetAndTimestamp.offset();  //不是通过time 换 offset，如果是从mysql读取回来，其本质是一样的
            System.out.println(offset);
            consumer.seek(partition, offset);

        }

//        try {
//            System.in.read();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }



        while (true) {
            //阻塞等待拉取消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));
            if (records.count() != 0) {
                System.out.println("-------------拉取了 " + records.count() + " 条数据------------------");
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    int partition = record.partition();
                    long offset = record.offset();

                    System.out.println("key :" + record.key() + " value :" + record.value() + " partition: " + partition + " offset:" + offset);
                }
            }

        }
    }
}

