/**
*  Copyright (c) 2014-2018 浩瀚深度 All Rights Reserved.
*
*author:    夏涛
*Create Time:2015年8月20日
*/
package kafkaTest;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
/**
 * 
 * TODO   读取配置文件的工具类
 * @author xiatao
 * @date 2015年9月18日
 *
 */
public class ReadProperties {
   
    public static Properties getAbsolutePathProp(String file){
        
        Properties prop=new Properties();
        
        try {
               InputStream in = new BufferedInputStream(new FileInputStream(file));
               
                prop.load(in);
               
           } catch (FileNotFoundException e) {
            
            e.printStackTrace();
            
        } catch (IOException e) {
           
            e.printStackTrace();
        }
        
        return prop;
    }
}

