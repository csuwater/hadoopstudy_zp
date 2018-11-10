package com.zp.hadoop.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: zhangpan@mlogcn.com
 * @Date: 2018/11/10 下午7:47
 * @Version 1.0
 * @Description
 */
public class MaxTemperatureReducer extends Reducer<Text ,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue=Integer.MIN_VALUE;
        for(IntWritable value:values)
        {
            maxValue=Math.max(maxValue,value.get());
        }
        context.write(key,new IntWritable(maxValue));

    }
}
