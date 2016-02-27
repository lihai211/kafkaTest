/**
 * Copyright (c) 2008-2015 浩瀚深度 All Rights Reserved.
 * 
 * FileName: MyExecutorReject.java
 * 
 * Description: TODO
 * 
 * History: v1.0.0, xuxiaolong, 2015年9月17日, Create
 */
package kafkaTest;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * ThreadPoolExecutor 类的拒绝处理。此处只打印 被拒绝的消息，以测试是否有拒绝处理
 * 
 * @author xiatao
 * @date 2015年9月17日
 * 
 */
public class MyExecutorReject implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {

        System.out.println("Reject by Executor:");
    }

}
