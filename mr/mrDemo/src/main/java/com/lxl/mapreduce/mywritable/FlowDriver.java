package com.lxl.mapreduce.mywritable;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputflow\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\output\\inputflow\\9"));

        boolean ret = job.waitForCompletion(true);
        System.exit(ret ? 0 : 1);



    }
}
