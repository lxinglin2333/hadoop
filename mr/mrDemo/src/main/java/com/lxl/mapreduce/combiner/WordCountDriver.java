package com.lxl.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        // 3 关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出的kV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setCombinerClass(WordCountCombiner.class);
//
//        job.setNumReduceTasks(0);
        /**shuffle之前可以选择添加combine提前聚合来减轻reducer压力
         * 因为一般都是map多redu少
         * combine只能适用于部分场景，比如求和，而求平均值这类影响到最后结果准确性的则不适合使用该功能*/
        job.setCombinerClass(WordCountReducer.class);
//
        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\input\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output\\combine\\1"));

        // 7 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}