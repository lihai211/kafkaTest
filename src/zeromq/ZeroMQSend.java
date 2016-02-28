/**
 * History:
 * v1.0.0, xiatao, 2015年10月23日, Create
 * 
 * @date 2015年10月23日
 *
 */

package zeromq;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

import kafkaTest.ReadProperties;

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
        if(args.length!=2)
        {
            System.out.println("请输入配置文件目录和要发送的文件:<配置文件目录(发送端口)>     <要发送的文件>");
            System.exit(1);
        }
        send.runSend(args[0], args[1]);
    }
}