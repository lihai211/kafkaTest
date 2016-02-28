package kafkaTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import tool.util.ReadProperties;

/***
 * 
 * kafka 消费者
 * 
 * @author xiatao
 * @date 2015年9月18日
 * 
 */
public class CommonConsumer implements Runnable {

    Properties props = new Properties();

    private String topicName = "";

    private int partionNum = 0;

    // 管理consumer与zookeeper的所有交互
    ConsumerConnector consumerConnector = null;

    private static int consumeNum = 0;

    /**
     * 
     * @Title: CommonConsumer
     * @Description: 构造方法，初始化一些参数
     * @param topicName
     *            需要去消费的topic名称
     * @param partionNum
     *            该topic有几个partition
     * @param configHead
     *            使用此Consumer时，读取的配置文件(consumer.properties)的父目录
     * @throws
     */
    public CommonConsumer(String topicName, int partionNum, String configHead) {

        this.topicName = topicName;

        this.partionNum = partionNum;

        String consumerPath = configHead + "/config/consumer.properties";

        props = ReadProperties.getAbsolutePathProp(consumerPath);
    }

    /**
     * 
     * 将消费过程放入线程
     */
    public void startConsumer() {

        // Create the connection to the cluster
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put(topicName, partionNum);

        // create list of 4 threads to consume from each of the partitions
        ExecutorService executor = Executors.newFixedThreadPool(partionNum);
        // threads to consume
        Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = consumerConnector
                .createMessageStreams(map);

        // KafkaStream<K,V> here K and V specify the type for the partition key and message value,
        List<KafkaStream<byte[], byte[]>> streams = topicMessageStreams.get(topicName);

        // consume the messages in the threads
        System.out.println("consumer start from , topicName=" + topicName + "\tstreams size=" + streams.size());

        for (final KafkaStream<byte[], byte[]> stream : streams) {

            executor.submit(new Runnable() {

                public void run() {

                    System.out.println("running in consumer thread!");

                    for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {

                        // System.out.println("now we have consumed message count="
                        // + consumeNum++);

                        consumeNum++;
                    }
                }
            });
        }

        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);

            System.out.println("Consumer  have completed all task!  consumed message sum=" + consumeNum);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void run() {

        startConsumer();
    }

    /**
     * 关闭Consumer
     */
    public void close() {
        try {
            consumerConnector.shutdown();
        } catch (Exception e) {

        }
    }

    /**
     * 
     * @return 消费者消费了多少条消息计数
     */
    public int getConsumeNum() {

        return consumeNum;
    }
}