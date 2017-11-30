package com.knightly.hadoop.spring;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 使用Spring Hadoop来访问HDFS文件系统
 * Created by knightly on 2017/11/30.
 */
public class SpringHadoopHDFSApp {
    private ApplicationContext ctx;
    FileSystem fileSystem;

    @Before
    public void setUp() {
        ctx = new ClassPathXmlApplicationContext("beans.xml");
        fileSystem = (FileSystem) ctx.getBean("fileSystem");
    }

    @After
    public void tearDown() {
        ctx = null;
        fileSystem=null;
    }

    /**
     * 创建HDFS文件夹
     * @throws Exception
     */
    @Test
    public void  testMkdir() throws Exception{
        fileSystem.mkdirs(new Path("/springhdfs/"));
    }

    /**
     * 上传文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception{
        Path localPath = new Path("D:/hello.txt");
        Path hdfsPath = new Path("/springhdfs/");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }

    /**
     * 查看HDFS文件的内容
     * @throws Exception
     */
    @Test
    public void cat() throws Exception{
        FSDataInputStream input = fileSystem.open(new Path("/springhdfs/hello.txt"));
        IOUtils.copyBytes(input,System.out,1024);
        input.close();
    }

}
