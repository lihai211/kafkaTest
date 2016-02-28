/**
*
*author:    夏涛
*Create Time:2015年9月16日
*/
package JustLearnHowToUseKafka;
import java.util.Properties;
import java.util.Random;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
/**
 * 
 * TODO  此类是测试producer的类。项目中未使用
 * @author xiatao
 * @date 2015年9月18日
 *
 */
public class SimpleProducer {
    private static Producer<String, String> producer;
    private final Properties props = new Properties();
    public SimpleProducer()
    {
    props.put("metadata.broker.list","Master:9092, Slave1:9093,Slave2:9092");
    props.put("serializer.class","kafka.serializer.StringEncoder");
    //props.put("partitioner.class", "test.kafka.SimplePartitioner");
    props.put("request.required.acks", "1");
    ProducerConfig config = new ProducerConfig(props);
    producer = new Producer<String, String>(config);
    }
    public static void main(String[] args) {
     SimpleProducer sp = new SimpleProducer();
    Random rnd = new Random();
    String topic = (String) args[0];
    for (long messCount = 0; messCount < 10; messCount++) {
    Integer key = rnd.nextInt(255);
    String msg = "This message is for key - " + key;
    KeyedMessage<String, String> data1 = new KeyedMessage<String, String>(topic, msg);
       
    producer.send(data1);
    
    System.out.println("message  send  is:"+msg+"[  topics >]"+topic);
    }
    producer.close();
    }
    }


