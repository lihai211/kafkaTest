/**
 * Copyright (c) 2014-2018 浩瀚深度 All Rights Reserved.
 * 
 * author: 夏涛 Create Time:2015年8月20日
 */
package kafkaTest;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 将从端口收到数据，用producer发送数据，consumer消费数据，整个过程定义为一条ProcessLine。顺序执行，整个流程为一个线程
 */
public class ProcessLine implements Runnable {

    ArrayBlockingQueue<String> msgQueue = null;
    // kafka安装目录
    String kafkaHome = "/root/kafka_2.9.2-0.8.2.1/";
    // UDP socket通信从哪个端口接收数据，可能会有多个
    String listenPorts = "";
    // UDP socket通信从哪个主机接收数据
    String listenhost = "172.16.20.111";
    // UDP 建立监听的主机名
    String listenHostName = "Master";
    // kafka的多个topics
    String topicsName = "";
    // kafka每个topic对应的分区数量，以逗号分隔
    String partitionNums = "";
    // UDP通信时，每个报文大小
    int datagramBufferSize = 0;
    // 此类和SocketReader共用一个配置文件，但是此类每次调用时都是处理一个kafka
    // topics，参数Index用来指明当前处理的是第几个topics(配置文件topics有多个，以逗号分隔)
    int index = -1; 
    // UDP接受者接收到 了多少条消息
    public int receiveMsgNum = 0;
    // UDP接受者的端口号
    public int receivePort = 0;
    // consumer消费了多少条消息
    public int consumerNum = 0;

    public ProcessLine(String configPath, ArrayBlockingQueue<String> msgQueue, int index) {
        init(configPath, msgQueue);

        this.index = index;
    }

    /**
     * 初始化各个参数
     * 
     * @param configPath
     *            配置文件目录
     */
    public void init(String configPath, ArrayBlockingQueue<String> queue) {

        Properties props = new Properties();

        props = ReadProperties.getAbsolutePathProp(configPath);

        topicsName = props.getProperty("kafkaTopicName");

        kafkaHome = props.getProperty("kafka_home");

        listenHostName = props.getProperty("listen_hostName");

        listenPorts = props.getProperty("listen_port");

        partitionNums = props.getProperty("partition_num");

        datagramBufferSize = Integer.parseInt(props.getProperty("datagram_bufferSize"));

        msgQueue = queue;
    }

    /**
     * 主要的执行方法
     * 
     * @param topicName
     *            当前需要处理的kafka topics
     * @param topicNum
     *            当前kafka topics有多少个partition
     * @param port
     *            从哪个端口接收数据 发送到此topics
     * @throws Exception
     */
    public void mainRun(String topicName, int topicNum, int port) throws Exception {

        ProducerUtil producer = new ProducerUtil(kafkaHome);

       // CommonConsumer comConsumer = new CommonConsumer(topicName, topicNum, kafkaHome);

        /**
         * 初始化读取端口的Socket的类Receive
         */
        Receive receive = new Receive(msgQueue);
        receive.init(port, listenHostName);
        System.out.println("receive started at port:" + port);
        // 启动ZeroMQ的接收端接收数据
        Thread zeromqSend = new Thread(receive);
        zeromqSend.start();
        receiveMsgNum = receive.getCount();
        receivePort = receive.getPort();
        // 启动kafka的Consumer程序
        //Thread consumerThread = new Thread(comConsumer);
        // consumerThread.start();
        long begin = 0;
        // 标记位
        int flag = 1;

        while (flag > 0) {
            /**
             * ConcurrentLinkedQueue的.size()是要遍历一遍集合的，比较慢，所以尽量要避免用size而改用isEmpty
             * ().
             */
            if (msgQueue.isEmpty()) {
                // 如果队列中已经有数据了，再进入此循环，说明队列中的数据已经空了。将标志设置为-1，跳出循环。  
                if ((flag == 2) && (receive.getIfReceiveFinished()==2)){
                    flag = -1;
                    /**
                     * 此时消息队列为空了，继续发送一次。通知producer发送其内部缓存队列中残存的消息
                     */
                    producer.send(topicName, msgQueue, begin);

                    // 获取最新的消费者消息数目
                    // consumerNum=comConsumer.getConsumeNum();

                   // System.out.println("kafka consumer have consumer num="+comConsumer.getConsumeNum());

                    double allMsgSize = (double) ((producer.getSendCount() * 467)) / (1024 * 1024)+1;

                    long end = System.currentTimeMillis();

                    long t = end - begin;

                    System.out.println("producer real send data sum=" + producer.getSendCount());

                    System.out.println("transfer size is:" + allMsgSize + "\t speed is:"
                            + ((double) allMsgSize / ((double) t / 1000)) + "MB/sec");

                    System.out.println("transfer real  speed is:" + (producer.getSendCount() / ((double) t / 1000))
                            + "records/sec");

                    break;
                }

            } else {
                // 首次产生了数据，且仅赋值给begin 一次
                if (flag == 1 && begin == 0)
                    begin = System.currentTimeMillis();
                flag = 2;

                producer.send(topicName, msgQueue, begin);
            }

        }
    }

    @Override
    public void run() {
        /**
         * 每条处理流水（一套处理过程）需要的参数：topicName,端口号,分区数(partition
         */
        String[] topicsArr = topicsName.split(",");

        String[] listenPortsArr = listenPorts.split(",");

        String[] partitionNumsArr = partitionNums.split(",");

        try {
            System.out.println("ProcessLine start to run!");

            mainRun(topicsArr[index], Integer.parseInt(partitionNumsArr[index]),
                    Integer.parseInt(listenPortsArr[index]));

        } catch (NumberFormatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

   
    public int getReceiveCount() {
        return receiveMsgNum;
    }

    public int getReceivePort() {
        return receivePort;
    }

    public int getComConsumerNum() {
        return consumerNum;
    }
}
