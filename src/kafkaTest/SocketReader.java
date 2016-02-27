/**
 * Copyright (c) 2014-2018 浩瀚深度 All Rights Reserved.
 * 
 * author: 夏涛 Create Time:2015年8月17日
 */
package kafkaTest;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 
 * TODO 项目的执行主类。main函数入口类。
 * 
 * @author xiatao
 * @date 2015年9月18日
 * 
 */
public class SocketReader {

    int topic_num = 0;
   /**
    * 
    * TODO       
    * @param configPath   kafka 集群配置文件的父目录
    */
    public void runAllTopics(String configPath) {

        Properties props = new Properties();

        props = ReadProperties.getAbsolutePathProp(configPath);

        String key = "topic_num";

        topic_num = Integer.parseInt(props.getProperty(key));

        ExecutorService executor = Executors.newFixedThreadPool(topic_num);

        for (int i = 0; i < topic_num; i++) {
            ArrayBlockingQueue<String> msgQueue = new ArrayBlockingQueue<String>(1000000);

            ProcessLine oneLine = new ProcessLine(configPath, msgQueue, i);

            executor.submit(oneLine);

            System.out.println("the process line thread  number is:" + i);

            // System.out.println("send message from producer,count num="+oneLine.getProduerMessageNum()+"  port="+i+"  now Queue size is:"+msgQueue.size());

            System.out.println("ZeroMQ  receive message number is:" + oneLine.getReceiveCount() + "  from port:"
                    + oneLine.getReceivePort());

            System.out.println("kafka consumer have consumer num=" + oneLine.getComConsumerNum());
        }

    }

    public static void main(String[] args) {

        SocketReader sr = new SocketReader();

        String configPath = "/home/xiatao/kafka/config/config.properties";

        sr.runAllTopics(configPath);
    }
}
