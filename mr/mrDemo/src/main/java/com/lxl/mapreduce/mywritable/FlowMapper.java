package com.lxl.mapreduce.mywritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    private Text outk = new Text();
    private  FlowBean outv = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();

      String[] split = line.split("\t");

        String phone = split[1];
        String up = split[split.length - 3];
        String down  = split[split.length - 2];

      outk.set(phone);

      outv.setUpFlow(Long.parseLong(up));
      outv.setDownFlow(Long.parseLong(down));
      outv.setSumFlow();

      context.write(outk,outv);
    }
}
