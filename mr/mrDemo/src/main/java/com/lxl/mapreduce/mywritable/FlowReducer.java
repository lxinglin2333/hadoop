package com.lxl.mapreduce.mywritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean, Text,FlowBean> {
    private  FlowBean outv = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long totalUp = 0;
        long totalDown = 0;
        for(FlowBean value : values){
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }

        outv.setUpFlow(totalUp);
        outv.setDownFlow(totalDown);
        outv.setSumFlow();

        context.write(key,outv);
    }
}
