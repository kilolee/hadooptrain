package com.knightly.hadoop.mapreduce.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * Hadoop HDFS Java API 操作
 * Created by knightly on 2017/11/27.
 */
public class HDFSApp {
    public static final String HDFS_PATH="hdfs://centos5:8020";

    FileSystem fileSystem =null;
    Configuration configuration=null;

    @Before
    public void setUp() throws Exception{
        System.out.println("HDFSApp.setUp");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"root");

    }

    @After
    public void tearDown() throws Exception{
        System.out.println("HDFSApp.tearDown");
        configuration =null;
        fileSystem=null;
    }

    /**
     *创建HDFS目录
     */
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception{
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }

    /**
     * 查看HDFS文件的内容
     * @throws Exception
     */
    @Test
    public void cat() throws Exception{
        FSDataInputStream input = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(input,System.out,1024);
        input.close();
    }

    /**
     * 重命名
     * @throws Exception
     */
    @Test
    public void rename() throws Exception{
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath,newPath);
    }

    /**
     * 上传文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception{
        Path localPath = new Path("D:/hello.txt");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }

    /**
     * 上传大文件到HDFS,带进度条
     * @throws Exception
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception{
        InputStream input =new BufferedInputStream(new FileInputStream(new File("D:/spark-2.2.0-bin-without-hadoop.tgz")));
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/spark-2.2.0.tgz"), new Progressable() {
            public void progress() {
                System.out.print(".");//带进度条提醒信息
            }
        });
        IOUtils.copyBytes(input,output,4096);
    }

    /**
     * 下载HDFS文件到本地
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception{
        Path localPath = new Path("D:/h.txt");
        Path hdfsPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.copyToLocalFile(hdfsPath,localPath);
    }

    /**
     * 输出文件列表
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹":"文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();

            System.out.println(isDir +"\t"+replication +"\t" + len + "\t" + path);
        }

    }

    /**
     * 删除
     * @throws Exception
     */
    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/hdfsapi/test/hello.txt"),true);
    }
}
