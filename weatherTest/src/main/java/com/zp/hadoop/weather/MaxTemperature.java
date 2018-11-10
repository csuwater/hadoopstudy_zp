package com.zp.hadoop.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 * @Author: zhangpan@mlogcn.com
 * @Date: 2018/11/10 下午7:54
 * @Version 1.0
 * @Description
 */
public class MaxTemperature {


    public static  void main(String [] args)throws  Exception{
//        if(args.length!=2)
//        {
//            System.err.print("数据路径错误");
//            System.exit(-1);
//
//        }
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf, "Max temperature");
        job.setJarByClass(MaxTemperature.class);
        FileInputFormat.addInputPath(job,new Path("hdfs://localhost:8020/weather/1901.txt"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://localhost:8020/output7"));
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true)?0:1);

    }
}
