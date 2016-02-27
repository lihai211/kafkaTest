/**
 * Copyright (c) 2008-2015 浩瀚深度 All Rights Reserved.
 * 
 * FileName: hwclient.java
 * 
 * Description: TODO
 * 
 * History:
 * v1.0.0, xiatao, 2015年10月23日, Create
 */
package zeromq;


import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

import kafkaTest.ReadProperties;


/**
 * TODO
 * @author xiatao
 * @date 2015年10月23日
 *
 */
import org.zeromq.ZMQ;

public class ZeroMQSend {
 
    String listenHome="";
    String port="";
    
    public void runSend(String configPath,String sendFile){
        
        init(configPath);
        ZMQ.Context context = ZMQ.context(1);   
        // Socket to talk to server
        ZMQ.Socket socket  = context.socket(ZMQ.PUSH);
        String conStr="tcp://"+listenHome+":"+port;
        
        System.out.println("Connecting ZEROMQ server\t"+conStr);
        socket .bind (conStr);
      
        ArrayList<String> sendFileContent=ReadFile.readFileByLines(sendFile);
        
        for(int requestNbr = 0; requestNbr != sendFileContent.size(); requestNbr++) {
          
            socket .send(sendFileContent.get(requestNbr).getBytes (),0);
            
            //System.out.println("send file:"+sendFileContent.get(requestNbr));
           /**
            byte[] reply = socket.recv(0);
            System.out.println("Received " + new String (reply) + " " + requestNbr);
            **/
        }
      System.out.println("文件发送完成！");
      socket .close();
        context.term();
    }

    /**
     * 初始化各个参数
     * 
     * @param configPath
     *            配置文件目录
     */
    public void init(String configPath) {

        Properties props = new Properties();

        props = ReadProperties.getAbsolutePathProp(configPath);

        listenHome = props.getProperty("listen_host");

        port = props.getProperty("listen_port");
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
        ZeroMQSend send=new ZeroMQSend();
        String s1="F:/xiataoWorkSpace/kafkaTest/config/config.properties";
        String s2="f:/3rdfile.txt";
       String s="d1435169399.8510041435169399.773288    1435169399.851004   460110246741101 3569540691068903    8615305564995   ctlte.mnc011.mcc460.gprs    6   1940486815  0   27908   114307891   1682324333  2362286174  6   63752   80  0   0   0   0   0   0   POST    adash.m.taobao.com  /rest/sur?ak=21380790&av=5.2.9&c=201200%40taobao_iphone_5.2.9&v=3.0&s=38d02f1547b018f193e357096e1c9a303c526c76&d=VLkKnAy1AEEDABc            1   200 0   78  text/plain  0   ";
       //System.out.println("一条话单长度为:"+s.length());
     
        if(args.length!=2)
        {
            System.out.println("请输入配置文件目录和要发送的文件:<配置文件目录(发送端口)>     <要发送的文件>");
            System.exit(1);
        }
        
        send.runSend(args[0], args[1]);
     
    }
}