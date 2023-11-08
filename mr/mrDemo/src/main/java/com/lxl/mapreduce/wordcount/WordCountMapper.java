package com.lxl.mapreduce.wordcount;

import jdk.nashorn.internal.runtime.regexp.joni.exception.InternalException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {


    private  Text outk = new Text();
    private  IntWritable outv = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");

        for (String word : words){
            outk.set(word);
            context.write(outk,outv);

        }
    }
}
