/**
 * 
 * FileName: SimplePartitioner.java
 * 
 * Description: TODO
 * 
 * History: v1.0.0, xuxiaolong, 2015年9月17日, Create
 */
package JustLearnHowToUseKafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * kafka的producer发送消息 分区 辅助类
 * 可以重载原始的Partitioner 实现自己的分区
 * 
 * @author xiatao
 * @date 2015年9月17日
 * 
 */
//import kafka.producer.Partitioner;

public class SimplePartitioner implements Partitioner {

    public SimplePartitioner (VerifiableProperties props) {
        
    }
 
   @Override
    public int partition(Object key, int numPartitions) {
        // TODO Auto-generated method stub
        int partition = 0;
        int iKey = Integer.parseInt((String)key);
        
        if (iKey > 0) {
            partition = iKey % numPartitions;
        }
        return partition;
    }

} 

