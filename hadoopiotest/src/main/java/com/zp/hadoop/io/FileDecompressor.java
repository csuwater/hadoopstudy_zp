package com.zp.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * @Author: zhangpan@mlogcn.com
 * @Date: 2018/11/11 上午10:03
 * @Version 1.0
 * @Description  通过CompressionCodeFactory 推断 CompressionCodec
 */
public class FileDecompressor {

    public static  void  main(String []args) throws  Exception{
        String url="hdfs://localhost:8020/";
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(URI.create(url),conf);
        Path inputPath=new Path(url);
        CompressionCodecFactory factory=new CompressionCodecFactory(conf);
        CompressionCodec codec=factory.getCodec(inputPath);
        if(codec==null)
        {
            System.err.print("NO codec found");
        }

        String outputUrl=CompressionCodecFactory.removeSuffix(url,codec.getDefaultExtension());
        InputStream in=null;
        OutputStream out=null;
        try{
            in=codec.createInputStream(fs.open(inputPath));
            out=fs.create(new Path(outputUrl));
            IOUtils.copyBytes(in,out,conf);
        }finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }

    }
}
