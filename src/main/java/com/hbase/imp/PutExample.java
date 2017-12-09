package com.hbase.imp;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * HBase put写入数据到hbase
 */
public class PutExample {
    /**
     * @param args
     * @throws IOException
     */
    private HTable table = HTableUtil.getHTable("testtable");

    public static void main(String[] args) throws IOException {
        PutExample pe = new PutExample();
        pe.putRows();

    }

    public void putRows(){
        List<Put> puts = new ArrayList<Put>();
        for(int i=0;i<10;i++){
            Put put = new Put(Bytes.toBytes("row_"+i));
            Random random = new Random();

            if(random.nextBoolean()){
                put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("colfam1_qual1_value_"+i));
            }
            if(random.nextBoolean()){
                put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("colfam1_qual1_value_"+i));
            }
            if(random.nextBoolean()){
                put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"), Bytes.toBytes("colfam1_qual1_value_"+i));
            }
            if(random.nextBoolean()){
                put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual4"), Bytes.toBytes("colfam1_qual1_value_"+i));
            }
            if(random.nextBoolean()){
                put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual5"), Bytes.toBytes("colfam1_qual1_value_"+i));
            }
            puts.add(put);
        }
        try{
            table.put(puts);
            table.close();
        }catch(Exception e){
            e.printStackTrace();
            return ;
        }
        System.out.println("done put rows");
    }
}
