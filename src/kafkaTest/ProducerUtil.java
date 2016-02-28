
package kafkaTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import tool.util.ReadProperties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * 
 * TODO   
 * @author xiatao
 * @date 2015年9月18日
 *
 */
public class ProducerUtil  {
    private static  final Log  LOG = LogFactory.getLog(ProducerUtil.class); 
    /**
     * kafka producer读取的配置文件的父目录
     */
    private String configPath="";
    /**
     * producer 发送消息使用的API 类
     */
    private Producer<String,String> inner;
    /**
     * 发送的消息计数器。全局计数器
     */
    private static int count=0;
   /**
    * 本地计数器。由于线程的执行延迟，全局计数器可能 不能反映实际数目
    */
    private static int localCount=0;
    /**
     * 本地缓存队列，满1000条一起发送
     */
    ArrayList<KeyedMessage<String, String>> batch_Queue=new  ArrayList<KeyedMessage<String,String>>();
    /**
     * 缓存队列 一次发送消息数目
     */
    private static final int batch_num=100000;
     //发送数据线程池
    private static final int coreProducerThreadNum=400;
   //kafka 发送数据的最大线程数
    private static final int maxProducerThreadNum=1000;
    
    final CountDownLatch  totalThreadSingal = new CountDownLatch(maxProducerThreadNum);
    //使用线程池发送小消息
    private ThreadPoolExecutor executorProducer = new ThreadPoolExecutor(coreProducerThreadNum, maxProducerThreadNum,
            10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000) ,new MyExecutorReject());
     /**
      * kafka  producer构造方法
      * @Title: ProducerUtil
      * @Description: TODO
      * @param configHead    调用producer的配置文件路径
      * @throws
      */
    public ProducerUtil(String configHead){
        
        try{
               this.configPath=configHead;

               String producerPath=configPath+"config/producer.properties";
               
               System.out.println("the config path we get is："+producerPath);
               
               Properties prop=ReadProperties.getAbsolutePathProp(producerPath);
               
               prop.put("partitioner.class", "kafkaTest.SimplePartitioner");
               
               prop.put("acks", "0");
               prop.put("producer.type", "async");// 默认位sync，即同步
               prop.put("batch.num.messages", "1024");// 只有设为async异步时，这个才有效
               prop.put("max.request.size", "104857600");
               prop.put("send.buffer.bytes", "104857600");
               
                ProducerConfig config = new ProducerConfig(prop);
              
                //config.kafka$producer$async$AsyncProducerConfig$_setter_$batchNumMessages_$eq(300);
                             
                inner = new Producer<String, String>(config); 
            
        }catch(Exception e){
               System.out.println("load configure file error(from ProducerUtil):"+e);
               
               e.printStackTrace();
        }
    }
    /**
     * 发送kafka消息，一次发送一个容器的数据
     * @param topicName     要发送到的kafka  topic名称
     * @param messages         需要发送的数据
     */
    public void send(String topicName, ArrayBlockingQueue<String> msgQue,long t) {
               
      //  System.out.println("ProducerUtil run send  method!");
        
        if(executorProducer.getQueue().size()<maxProducerThreadNum)
        {               
            //凑够batch_Queue 的消息数目再一起发送
            if((batch_Queue.size()<batch_num)&&(!msgQue.isEmpty()))
            {
                ArrayList<String>  tmpMsg=new ArrayList<String>();
                      
                 msgQue.drainTo(tmpMsg);
            
               for(String msg:tmpMsg)
               {
                   //batch_Queue.add(new KeyedMessage<String, String>(topicName,count+"",msg));
                   
                   batch_Queue.add(new KeyedMessage<String, String>(topicName,count+"",msg));
               }
            }
            //达到发送数目。或者是最后一次（msgQue为空时 会继续发送一次，此时发送batch_Queue中残存的消息）
            else 
            {
                 SendMessageMuiltiThread sendThread=new SendMessageMuiltiThread(batch_Queue,t);
                
                executorProducer.submit(sendThread);
                
                //如果是发送 缓存队列中的 剩余消息，打印一条消息
                if(msgQue.isEmpty())
                    
                    System.out.println("send last message queue,batch_Queue  size is:"+batch_Queue.size()+"\tlocal count(before add batch_queue size)="+localCount);
            
               localCount+=batch_Queue.size();
            
                 //发送完之后 清空本地缓存队列
                batch_Queue.clear();
            }
   
        }
       else
       {
           System.out.println("task wait for once!");
           
           try{
                Thread.sleep(20);
           }catch(Exception e){
               e.printStackTrace();
           }
       }
    }
    
    public void close(){               
        inner.close();       
    }

    /**
     * 发送多条消息
     * 
     * @param topicName
     * @param messages
    
    public void sendMultiMess(String topicName, Collection<String> messages) {

        if (topicName == null || messages == null) {
            return;
        }
        if (messages.isEmpty()) {
            return;
        }
        List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();

        for (String entry : messages) {

            KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, entry);

            kms.add(km);

        }

        inner.send(kms);
        
        count+=kms.size();
    }
    
     */
    /**
     *   
     * TODO   producer发送消息的内部类，线程
     * @author xiatao
     * @date 2015年9月18日
     *
     */
    private  class SendMessageMuiltiThread implements Runnable {
        // 需要发送的队列
        private ArrayList<KeyedMessage<String, String>> messQue = new ArrayList<KeyedMessage<String, String>>();

         long begin=0;
        /**
         * 设置需要发送的消息和topic名称
         * 
         * @param messQue
         *            需要发送的消息
         * @param topicsName
         *            消息将要发送到的topic名称
         */
        public SendMessageMuiltiThread(ArrayList<KeyedMessage<String, String>> messQ,long beginTime) {

            messQue.addAll(messQ);
            
            begin=beginTime;
        }

        @Override
        public void run() {
            
            inner.send(messQue);
            
            count+=messQue.size();
            
            long end=System.currentTimeMillis();
            
            double speed=(double)count/((end-begin)/1000);
            
            System.out.println(" >>>>in run method  send mess:"+messQue.size()+"\tcount is:"+count+"\tspeed is:"+speed+"  records /sec"+"   \t time cost="+(end-begin)/1000+"seconds");
        } 
    }
    /**
     * 
     * @return   返回kafka producer 实际发送消息数目
     */
    public int getSendCount(){
       
        return count;
    }
}

