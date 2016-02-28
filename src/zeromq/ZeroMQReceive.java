/**
 * 
 * History:
 * v1.0.0, xiatao, 2015年10月23日, Create
 */
package zeromq;


/**
 * TODO
 * @author xiatao
 * @date 2015年10月23日
 *
 */
import java.io.PrintWriter;
import java.io.StringWriter;
 
import org.zeromq.ZMQ;
 
//
// Hello World server in Java
// Binds REP socket to tcp://*:5555
// Expects "Hello" from client, replies with "World"
//
 
public class ZeroMQReceive {
 
    /**
     * @param args
     */
    public static void main(String[] args) {
        ZMQ.Context context = ZMQ.context(1);
        // Socket to talk to clients
        ZMQ.Socket socket = context.socket(ZMQ.REP);
        socket.bind ("tcp://*:5555");
        try {
            while (!Thread.currentThread ().isInterrupted ()) {
                byte[] reply = socket.recv(0);
                System.out.println("Received Hello");
                String request = "World" ;
                socket.send(request.getBytes (), 0);
                Thread.sleep(1000); // Do some 'work'
            }
        } catch(Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            System.out.println(sw.toString());
        }
        socket.close();
        context.term();
 
    }
 
}