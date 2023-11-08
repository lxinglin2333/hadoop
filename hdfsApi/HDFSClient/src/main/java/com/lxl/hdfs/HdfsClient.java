package com.lxl.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    private FileSystem fs;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {

        //连接集群的nn地址
        URI uri = new URI("hdfs://hadoop01:9000");

        //创建一个配置文件
        Configuration configuration = new Configuration();

        //用户，指定用户，不然没权限会报错
        String user = "root";

        // 1 获取客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        // 3 关闭资源
        fs.close();
    }
    @Test
    public void testmkdir() throws  URISyntaxException,IOException,InterruptedException {
        // 2 创建一个文件夹
        fs.mkdirs(new Path("/test/109"));

    }
    /***
    * 参数优先级排序：
   （1）客户端代码中设置的值 >
   （2）ClassPath 下的用户自定义配置文件 >
   （3） 然后是服务器的自定义配置 （xxx-site.xml） >
   （4） 服务器的默认配置 （xxx-default.xml）***/
    // 上传
    @Test
    public void testPut() throws IOException {
        //参数解读：参数一：表示删除原数据；参数二：是否允许覆盖；参数三：原数据路径；参数四：目的地路径
        fs.copyFromLocalFile(false,true,new Path("D:\\Iron_Man.txt"),new Path("hdfs://hadoop01:9000/test/109"));
    }


   // 文件下载
    @Test
    public void testGet() throws IOException {
        //参数解读：参数一： boolean delSrc 指是否将原文件删除；参数二：Path src 指要下载的原文件路径
        // 参数三：Path dst 指将文件下载到的目标地址路径；参数四：boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("hdfs://hadoop01:9000/test/109/Iron_Man.txt"), new Path("D:\\Robert.txt"), false);
    }

    // 删除
    @Test
    public void testRm() throws IOException {

        // 参数解读：参数1：要删除的路径； 参数2 ： 是否递归删除
        // 删除文件(不再演示了)
        fs.delete(new Path("/output/18/part-r-00000"),false);

        // 删除空目录
        fs.delete(new Path("/output/10/_temporary/1"), false);

        // 删除非空目录
        fs.delete(new Path("/output/17"), true);
    }

    // 文件的更名和移动
    @Test
    public void testmv() throws IOException {
        // 参数解读：参数1 ：原文件路径； 参数2 ：目标文件路径
        // 对文件名称的修改
        fs.rename(new Path("/move/from.txt"), new Path("/move/new.txt"));

        // 文件的移动和更名
        fs.rename(new Path("/move/new.txt"),new Path("/to.txt"));

        // 目录更名
        fs.rename(new Path("/move"), new Path("/shift"));

    }

    // 判断是文件夹还是文件
    @Test
    public void testFile() throws IOException {

        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus status : listStatus) {

            if (status.isFile()) {
                System.out.println("文件：" + status.getPath().getName());
            } else {
                System.out.println("目录：" + status.getPath().getName());
            }
        }
    }


}
