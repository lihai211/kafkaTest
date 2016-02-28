## 说明
      此代码为测试kafka收发数据性能，仅测试了kafka producer发送数据性能，Consumer部分代码已经完成（被注释，没有执行）。
##  测试数据
      总数: 188181条，平均每条大小 467字节

## Kafka集群配置：
    <ol> 
     <li> Master : 单核 8 处理器，2GB内存（可用：1GB）</li>
     <li>Slave1: 双核，16处理器，7.8GB（可用159MB）</li>
     <li>Slave2：单核 ，3处理器，9.8GB（可用157MB）</li>
    </ol>
##  使用的框架:
    <p>ZeroMQ ( PUB / SUB)</p>    
     Kafka Producer (接收来自ZeroMQ的消息，放入本地的缓存队列（最大容量为100万，Producer使用线程池发送消息到kafka集群（3台主机）），线程池核心线程数200，最大线程数1000，缓存任务队列2000)
        Consumer：3个partition，每个partition 4个线程去消费

##  结果：
    <p> ZeroMQ 发送 188181条消息 平均时间消耗：1522ms</p>
    <p>Kafka Producer发送188181条消息，20931条/秒 ，平均9.5MB/秒</p>
