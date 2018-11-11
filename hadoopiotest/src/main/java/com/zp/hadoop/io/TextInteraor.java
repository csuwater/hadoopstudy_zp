package com.zp.hadoop.io;

import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;

/**
 * @Author: zhangpan@mlogcn.com
 * @Date: 2018/11/11 上午10:45
 * @Version 1.0
 * @Description  遍历Text中的字符
 */
public class TextInteraor {

    public static  void main(String [] args)
    {
        Text text=new Text("\u0041\u00DF\u6771\uD801\uDC00");
        //新的缓冲区长度为array的长度，实际存储的字节为array的实际字节，新的缓冲区从start开始放入字节，放入的字节长度不能超过原先放过字节剩余的长度
        ByteBuffer buf=ByteBuffer.wrap(text.getBytes(),0,text.getLength());
        int cp ;
        //hasRemaining 获取剩余的可用长度
        while(buf.hasRemaining()&&(cp = Text.bytesToCodePoint(buf))!=-1)
        {
            System.out.println(Integer.toHexString(cp));
        }
    }
}
