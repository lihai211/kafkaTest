package kafkaTest;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

/**
 *  Copyright (c) 2014-2018 浩瀚深度 All Rights Reserved.
 *
 *author:    夏涛
 *Create Time:2015年9月1日
 */
public class ReplaceFileNull {
       
    public static void main(String[] args){
        
        ReplaceFileNull r=new ReplaceFileNull();
        
        String file="f:\\part-m-00000";
        
        String val="100";
        
        r.replace(file, val);
    }
 public void replace(String file,String val){
        
        File f=new File(file);
        
        StringBuilder sb=new StringBuilder();
        
        try {
            BufferedReader  br=new BufferedReader(new FileReader(f));
            
            String line="";
            
            while((line=br.readLine())!=null)
            {
                String nn="";
                
                String[] strs=line.split("\\|");
                for(String s:strs)
                {
                    if(null==s||s.length()<1)
                         nn=nn+"|"+val;
                    else
                        nn=nn+"|"+s;
                }
                
                nn=nn.substring(1, nn.length());
                
                sb.append(nn);
                
                sb.append("\r\n");
            }
            
            FileOutputStream in = new FileOutputStream(file+"_done");                       
            
            in.write(sb.toString().getBytes(), 0,sb.toString().length());  
            
            in.close();  
            
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
}

