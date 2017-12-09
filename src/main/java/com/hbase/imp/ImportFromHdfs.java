package com.hbase.imp;

import org.apache.commons.cli.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 2 从hdfs文件导入到HBase
 */
public class ImportFromHdfs {

    public static final String NAME="ImportFromFile";
    public enum Counters{LINES}

    static class ImportMapper extends Mapper<LongWritable,Text, ImmutableBytesWritable,Writable>{
        private byte[] family =null;
        private byte[] qualifier = null;
        @Override
        protected void setup(Context cxt){
            String column = cxt.getConfiguration().get("conf.column");
            byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
            family = colkey[0];
            if(colkey.length>1){
                qualifier = colkey[1];
            }
        }
        @Override
        public void map(LongWritable offset,Text line,Context cxt){
            try{
                String lineString= line.toString();
                byte[] rowkey= DigestUtils.md5(lineString);
                Put put = new Put(rowkey);
                put.add(family,qualifier,Bytes.toBytes(lineString));
                //cxt.write(new ImmutableBytesWritable(rowkey), put);
                cxt.getCounter(Counters.LINES).increment(1);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    private static CommandLine parseArgs(String[] args){
        Options options = new Options();
        Option o = new Option("t" ,"table",true,"table to import into (must exist)");
        o.setArgName("table-name");
        o.setRequired(true);
        options.addOption(o);

        o= new Option("c","column",true,"column to store row data into");
        o.setArgName("family:qualifier");
        o.setRequired(true);
        options.addOption(o);

        o = new Option("i", "input", true,
                "the directory or file to read from");
        o.setArgName("path-in-HDFS");
        o.setRequired(true);
        options.addOption(o);
        options.addOption("d", "debug", false, "switch on DEBUG log level");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(NAME + " ", options, true);
            System.exit(-1);
        }
        return cmd;
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = HTableUtil.getConf();
        String[] otherArgs = new GenericOptionsParser(conf, initialArg()).getRemainingArgs();
        CommandLine cmd = parseArgs(otherArgs);
        String table = cmd.getOptionValue("t");
        String input = cmd.getOptionValue("i");
        String column = cmd.getOptionValue("c");
        conf.set("conf.column", column);
        Job job = new Job(conf, "Import from file " + input + " into table " + table);
        job.setJarByClass(ImportFromHdfs.class);
        job.setMapperClass(ImportMapper.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(input));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static String[] initialArg(){
        String []args = new String[6];
        args[0]="-c";
        args[1]="fam:data";
        args[2]="-i";
        args[3]="/user/hadoop/input/picdata";
        args[4]="-t";
        args[5]="testtable";
        return args;
    }
}
