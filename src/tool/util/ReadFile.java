/**
 * 
 * History:
 * v1.0.0, xiatao, 2015年10月23日, Create
 */
package tool.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;


/**
 * TODO
 * @author xiatao
 * @date 2015年10月23日
 *
 */
public class ReadFile {
   
    /**
     * 以行为单位读取文件，常用于读面向行的格式化文件
     */
    public static ArrayList<String>  readFileByLines(String fileName) {
        File file = new File(fileName);
        ArrayList<String>  fileResult=new ArrayList<String>();
        StringBuilder sb=new StringBuilder();
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号             
                fileResult.add(tempString);             
            }
            reader.close();
            //写入到新文件中
            //appendMethodA(newFile,sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return fileResult;
    }
    
    /**
     * A方法追加文件：使用RandomAccessFile
     */
    public static void appendMethodA(String fileName, String content) {
        try {
            // 打开一个随机访问文件流，按读写方式
            RandomAccessFile randomFile = new RandomAccessFile(fileName, "rw");
            // 文件长度，字节数
            long fileLength = randomFile.length();
            //将写文件指针移到文件尾。
            randomFile.seek(fileLength);
            randomFile.writeBytes(content);
            randomFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    
    public static void main(String[] args) throws FileNotFoundException{
        String file="f:/3rdpartyflow_20150625_021000_001053.log";
        String newFile="f:/3rdfile.txt";
        ReadFile read=new ReadFile();
        //read.read(file);
        //readFileByLines(newFile,"f:/3rdfile.txt");
    }
}
