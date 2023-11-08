package com.lxl.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        /**
         * 默认本地运行
         * 如果resource中包含mapred-site.xml和yarn-site.xml
         * 就需要添加下面两行来判断是在集群上还是本地运行
         * 优先级 代码>resources>集群配置>默认配置*/
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        //设置job
        Job job = Job.getInstance(conf);
        //确定驱动类
        job.setJarByClass(WordCountDriver.class);

        //确定map和redu
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //确定输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入输出
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputword"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\output\\wordcount\\1"));

        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);



    }
}
