package com.lxl.mapreduce.mywritable;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    //定义属性
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    //对外提供方法
    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow+this.downFlow;
    }

    //构造方法 空白处右键-->Generate-->Constructor
    public FlowBean() {
    }

    //重写方法，进行序列化和反序列化（选中报错处alt+L）
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);

    }

    public void readFields(DataInput dataInput) throws IOException {

        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }
    //重写方法toString
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
