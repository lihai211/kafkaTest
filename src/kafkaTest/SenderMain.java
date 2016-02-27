/**
*  Copyright (c) 2014-2018 浩瀚深度 All Rights Reserved.
*
*author:    夏涛
*Create Time:2015年8月17日
*/
package kafkaTest;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;  
import java.net.DatagramSocket;  
import java.net.InetAddress;  
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
 /**
  * 
  * TODO  向端口发送数据，此类先读取文件，然后发送一行行数据
  * @author xiatao
  * @date 2015年9月18日
  *
  */
public class SenderMain{
    
public class Sender implements Runnable{  
    
     byte[] ipByte=new byte[4];
    
    public static final String fromServer="Master";

    public String filePath="";
    
    public int port=0;
    
    public void init(String ipStr){
          
           String[] ipStrs=ipStr.split("\\.");
           
           for(int i=0;i<ipStrs.length;i++)
               
                this.ipByte[i]=(byte)Integer.parseInt(ipStrs[i]);
                 
    }
    
    /**
     * 
     * @param filePath
     * @param port
     * @param ipStr
     */
    public Sender(String filePath,int port,String ipStr){
        
           this.filePath=filePath;
           
           this.port=port;
           
           init(ipStr);
    }
    
    
    public void sendFileMessage(){
        
        try {  
            
            DatagramSocket sendSocket = new DatagramSocket();  
                       
            InetAddress ip= InetAddress.getByAddress(fromServer,ipByte);
            
            String strMess=readFile(filePath);
            
            String[] MessArr=strMess.split(",");
            
            int j=0;
            
            long begin=System.currentTimeMillis();
            
            for(String mess:MessArr)
            {
          
            byte[] buf = mess.getBytes();  
                        
            DatagramPacket sendPacket = new DatagramPacket(buf, buf.length, ip,  port);  
  
            sendSocket.send(sendPacket);
            
            j++;
            
          //  System.out.println("UDP 's Sender  send data:"+mess);
            }
           
            System.out.println("how many line read from file:"+j);
            
            long  end=System.currentTimeMillis();
            
            double timeConsume=(double)(end-begin)/1000;
            
            System.out.println("send message size="+strMess.length()/(1024*1024)+"MB  transfer speed is:"+(strMess.length()/(1024*1024))/timeConsume+" MB/sec time consume="+timeConsume);
            
            sendSocket.close();  
        } catch (Exception e) {  
            e.printStackTrace();  
        } 
      }
    
   //分批读取
    public String readFile(String file){
        
        File f=new File(file);
        
       // System.out.println("producer send file:"+file);
        
        int i=0;
        
        StringBuilder sb=new StringBuilder();
        try {
            
            BufferedReader br=new BufferedReader(new FileReader(f),4096);
            
            String line="";
            
            while((line=br.readLine())!=null&&i<200)
            {
             
                sb.append(line);
                
                 sb.append(",");              
                 
               //  i++;             
            }
            br.close();  
        } catch (FileNotFoundException e) {
            
               e.printStackTrace();
               
        } catch (IOException e) {
       
            e.printStackTrace();
        }
        return sb.toString();
    }
    
    /**
    public void sendMultiPortData(int port,String file){
        
       try {
             Thread.sleep(2000);
                
             sendFileMessage(file,port);
          } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
          }  
    }
    */
    @Override
    public void run() {
        sendFileMessage();
    }
}  
public static void main(String[] args){
    
    SenderMain main=new SenderMain();
 
    ExecutorService executor = Executors.newFixedThreadPool(20);
      
    String ip="172.16.21.100";
                        
    String[] files=new String[]{"F:/kafka_msg","F:/s1mme_msg","F:/s6a_msg"};
    
    int[] ports=new int[]{5555,5556,5557};
              
        Sender sender1=main.new Sender(files[0],ports[0],ip);
        
     //   Sender sender2=main.new Sender(files[1],ports[1],ip);
        
      //  Sender sender3=main.new Sender(files[2],ports[2],ip);
        
        executor.submit(sender1);
        
     //   executor.submit(sender2);
        
    //    executor.submit(sender3);
   
    

}

}

  