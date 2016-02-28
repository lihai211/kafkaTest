/**
*
*author:    夏涛
*Create Time:2015年8月17日
*/
package kafkaTest;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.zeromq.ZMQ;
 /**
  * 
  * TODO   接收消息的类。注意：之前使用的UDP协议接收数据，现在改用了ZeroMQ框架
  * @author xiatao
  * @date 2015年9月18日
  *
  */
public class Receive implements Runnable{  
    
    Queue<String> queue=null;
    
    public  int port=0;
    String fromServer="Master";     
    public static int count=0;
    /**
     * ZeroMQ接收到的消息计数
     */
    public static int receiveCount=0;
    /**
     * ZeroMQ接收消息是否完成.初始时 是 -1，代表尚未开始接受数据；接收数据时变为1；接收完成为2
     */
    private static int  receiveFinish=-1;
    /**
     * 使用ZeroMQ建立Socket连接
     */
    ZMQ.Socket zmqSocket =null;

    public Receive(ArrayBlockingQueue<String> q){
        this.queue=q;

    }
   /**
    * 初始化连接的参数 
    * @param port      需要从主机的哪个端口上读取数据
    * @param ipStr       需要从那个IP上读取数据
    * @param fromServer    从哪个主机上读取数据，该主机名
    */
   public void init(int port,String fromServer) {
       
       ZMQ.Context context = ZMQ.context(1);  
       // Socket to talk to clients
       zmqSocket= context.socket(ZMQ.PULL);
       String conStr="tcp://"+fromServer+":"+port;   
       System.out.println("init connect :"+conStr);       
       zmqSocket.connect (conStr);
      
   }
   long start = 0;
   long end = 0; 
   boolean isFirst = true;
@Override
public void run() {
    
    while(true)
    { 
        byte[] reply =zmqSocket.recv(0);      
        //如果当前要发送的消息非空，就认为是合法消息
        if(queue.size()<1000000)           
        {
        long t1=System.currentTimeMillis();
        
        while ((reply[0]!=0)) {
            
            reply= zmqSocket.recv(0);       
             queue.offer(new String(reply));
             count++;
             //接收数据
             receiveFinish=1;
             //System.out.println("send message from ZeroMQ:"+new String(reply));
        }
        //接收数据完成 
        if(receiveFinish==1)
            receiveFinish=2;       
        long t2=System.currentTimeMillis();      
        System.out.println("将ZeroMQ消息放入队列  时间消耗是："+(t2-t1));
        }     
        else if(queue.size()>=1000000)
        {
            System.out.println("the shared queue size beyond Max,100 0000 .make it sleep!");
            
            try {
                Thread.sleep(100);   //加个次数限制，此时数据会丢
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
    }
    
}
public int getCount(){
    return count;
}

public int getPort(){
    return port;
}

public int getIfReceiveFinished(){
    return receiveFinish;
}
}  
