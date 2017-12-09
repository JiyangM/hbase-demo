package com.hbase.imp;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * mapreduce读取文本数据 存入hbase
 */
public class HbaseSinker {

    private static String table="table_name";

    static class HbaseSinkMrMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields=line.split("\t");
            String phone = fields[0];
            String url=fields[1];
            Text text = new Text();
            text.set(phone+"\t"+url);
            context.write(text,NullWritable.get());
        }
    }

    static class HbaseSinkMrReducer extends TableReducer<Text,NullWritable,ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String[] fields =key.toString().split("\t");
            byte[] rowkey=fields[0].getBytes();
            Put put = new Put(rowkey);
            put.add("info".getBytes(),"url".getBytes(),fields[1].getBytes());
            context.write(new ImmutableBytesWritable(rowkey),put);
        }
    }


    public static void main(String[] args) throws Exception{
        org.apache.hadoop.conf.Configuration conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","mini2:2181");
        Job job=Job.getInstance(conf);

        job.setJarByClass(HbaseReader.class);
        Scan scan = new Scan();

        job.setReducerClass(HbaseReader.HdfsSinkreducer.class);
        FileOutputFormat.setOutputPath(job,new Path("D:\\tmp\\test"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        TableMapReduceUtil.initTableMapperJob(table,scan,HbaseReader.HdfsSinkMapper.class,Text.class,NullWritable.class,job);
        job.waitForCompletion(true);
    }
}
