package com.hbase.imp;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * mapreduce 从hbase中读取数据
 */
public class HbaseReader {

    private static String t_user_info="testtable";

    static class HdfsSinkMapper extends TableMapper<Text,NullWritable>{

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte[] bytes=key.copyBytes();
            String rowkey= new String(bytes);
            System.out.println("--------------------------");
            byte[] usernameBytes=value.getValue(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"));
            if(usernameBytes !=null){
                String username=  new String(usernameBytes);
                System.out.println("===========================");
                context.write(new Text(rowkey+"\t"+username),NullWritable.get());
            }
        }
    }

    static class HdfsSinkreducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception{
        org.apache.hadoop.conf.Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","mini2:2181");
        Job job=Job.getInstance(conf);

        job.setJarByClass(HbaseReader.class);
        Scan scan = new Scan();

        job.setReducerClass(HdfsSinkreducer.class);
        FileOutputFormat.setOutputPath(job,new Path("D:\\tmp\\test"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        TableMapReduceUtil.initTableMapperJob(t_user_info,scan,HdfsSinkMapper.class,Text.class,NullWritable.class,job);
        job.waitForCompletion(true);
    }

}
