package com.lxl.mapreduce.wordcounthdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        /**
         * 解决权限不足的问题*
         * 在win7系统的环境变量或java JVM变量里面添加 HADOOP_USER_NAME，
         * 值为运行HADOOP上的Linux的用户名。
         * 并重启idea/
         String user = "root";*/
         //指定mapreduce可以在远程集群上运行
        conf.set("mapreduce.app-submission.cross-platform", "true");
        //指定yarn的resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname", "hadoop01");

        /**本地运行
         * 想要在本地运行
         * resource添加yarn-site.xml等xml文件后必须添加
         * 没添加默认本地运行*/
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("mapreduce.framework.name", "local");


        /**hdfs上运行
         * resource添加yarn-site.xml等xml文件后可有可无
         * 没添加想要在集群运行需要添加下面两行*/
//        conf.set("fs.defaultFS","hdfs://192.168.56.104:9000");
//        conf.set("mapreduce.framework.name", "yarn");

        //Job job = Job.getInstance(conf);
        Job job = Job.getInstance(conf, "word_count");

        //job.setJarByClass(WordCountDriver.class);
        //远程向集群提交任务,因为集群上没有自己写的代码,因此要设置下jar包
        job.setJar("D:\\Project\\hadoop\\mr\\mrDemo\\target\\mrDemo-1.0-SNAPSHOT.jar");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
1
        //本地模式下的设置输入输出地址
        //FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputword"));

        FileInputFormat.setInputPaths(job,new Path("/data/wc/input/haha.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/data/wc/output/105"));

//        本地模式下进行一个判断，如果目录存在，则把本地上的输出目录删除
//        Path op = new Path("D:\\output\\3");
//        FileSystem fs = FileSystem.get(conf);
//        判断路径是否存在，存在则删除
//        if (fs.exists(op)) {
//            fs.delete(op, true);
//        }
//        FileOutputFormat.setOutputPath(job, op);

        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);



    }
}
