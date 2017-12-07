import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HbaseDemo {

	private Configuration conf = null;
	private Connection conn = null;

	@Before
	public void init() throws IOException {
		/**
		 * 对于hbase的客户端，只需要知道hbase所使用的zookeeper集群地址就可以了
		 * 因为hbase的客户端找hbase读写数据完全不经过hmaster
		 */
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "mini1");
		conf.set("hbase.rpc.timeout" , "60");
		conn = ConnectionFactory.createConnection(conf);
	}

	/**
	 * 创建表
	 * @throws IOException
	 */
	@Test
	public void testCreate() throws IOException {
		// 获取一个表管理器
		Admin admin = conn.getAdmin();
		// 构造一个表描述器，并指定表名称
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("t_user_info2"));
		// 构造一个列族描述器，并制定列族名
		HColumnDescriptor hcd1 = new HColumnDescriptor("base_info");
		// 为列族指定布隆过滤器类型，设定列族版本
		hcd1.setBloomFilterType(BloomType.ROW).setVersions(1, 3);

		// 构造一个列族描述器，并制定列族名
		HColumnDescriptor hcd2 = new HColumnDescriptor("extra_info"); 
		// 为列族指定布隆过滤器类型，设定列族版本
		hcd2.setBloomFilterType(BloomType.ROW).setVersions(1, 3);

		htd.addFamily(hcd1).addFamily(hcd2);
		admin.createTable(htd);
		admin.close();
		conn.close();
	}

	/**
	 * 删除表
	 * @throws Exception
	 */
	@Test
	public void testDrop() throws Exception{
		Admin admin=conn.getAdmin();
		admin.disableTable(TableName.valueOf("t_user_info2"));
		admin.deleteTable(TableName.valueOf("t_user_info2"));
		admin.close();
		conn.close();
	}

	/**
	 * 修改列族bloom过滤器，新加列族
	 * @throws Exception
	 */
	@Test
	public void testModify() throws Exception{
		Admin admin=conn.getAdmin();
		// 修改已有的ColumnFamily
		HTableDescriptor table=admin.getTableDescriptor(TableName.valueOf("t_user_info"));
		HColumnDescriptor hcd=table.getFamily("extra_info".getBytes());
		hcd.setBloomFilterType(BloomType.ROWCOL);
		hcd.setVersions(1,5);
		// 添加新的columnFamily
		table.addFamily(new HColumnDescriptor("other_info"));

		admin.modifyTable(TableName.valueOf("t_user_info"),table);
		admin.close();
		conn.close();
	}

	/**
	 * 添加数据put
	 * @throws Exception
	 */
	@Test
	public void testPut() throws Exception{
		Table table=conn.getTable(TableName.valueOf("t_user_info"));

		ArrayList<Put> puts = new ArrayList<Put>();

		// 构建put 对象的kv，并指定行健
		Put put01 = new Put(Bytes.toBytes("user03"));
		put01.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("username"),Bytes.toBytes("zhangsan"));

		Put put02 = new Put(Bytes.toBytes("user03"));
		put02.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("password"),Bytes.toBytes("123456"));

		Put put03 = new Put(Bytes.toBytes("user03"));
		put03.addColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("username"),Bytes.toBytes("zhangsan"));

		Put put04 = new Put(Bytes.toBytes("user03"));
		put04.addColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("password"),Bytes.toBytes("123456"));

		puts.add(put01);
		puts.add(put02);
		puts.add(put03);
		puts.add(put04);

		table.put(puts);
		table.close();
		conn.close();
	}

	/**
	 * 获取数据get
	 * @throws Exception
	 */
	@Test
	public void testGet() throws Exception{
		Table table=conn.getTable(TableName.valueOf("t_user_info"));
		// 构造一个get查询参数对象，指定get的是哪一行
		Get get=new Get("user01".getBytes());
		get.setMaxVersions(5);
		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		//result.getValue(family, qualifier);  可以从result中直接取出一个特定的value

		//遍历出result中所有的键值对
		List<KeyValue> kvs = result.list();
		//kv  ---> f1:title:superise....      f1:author:zhangsan    f1:content:asdfasldgkjsldg
		for(KeyValue kv : kvs){
			String family = new String(kv.getFamily());
			System.out.println(family);
			String qualifier = new String(kv.getQualifier());
			System.out.println(qualifier);
			System.out.println(new String(kv.getValue()));
		}
		table.close();
	}

	/**
	 * 删除列
	 * @throws Exception
	 */
	@Test
	public void testDel() throws Exception{
		Table table=conn.getTable(TableName.valueOf("t_user_info"));
		Delete delete= new Delete("user01".getBytes());
		delete.addColumn("base_info".getBytes(),"password".getBytes());
		table.delete(delete);
		table.close();
		conn.close();
	}

	/**
	 * scan 批量获取数据
	 * @throws Exception
	 */
	@Test
	public void testScan() throws Exception{
		Table table = conn.getTable(TableName.valueOf("t_user_info"));
		// Scan scan = new Scan();
		// scan 中可以增加查询条件（行健范围左闭右开，包含右部的方法+000）
		Scan scan = new Scan("user01".getBytes(),("user03"+"000").getBytes());
		ResultScanner scanner =  table.getScanner(scan);
		Iterator<Result> iterable = scanner.iterator();
		while(iterable.hasNext()){
			Result result = iterable.next();
			CellScanner cellScanner = result.cellScanner();
			while(cellScanner.advance()){
				Cell current = cellScanner.current();
				byte[] familyArray = current.getFamilyArray();
				byte[] valueArray = current.getValueArray();
				byte[] qualifierArray = current.getQualifierArray();
				byte[] rowArray = current.getRowArray();

				System.out.println(new String(rowArray,current.getRowOffset(),current.getRowLength()));
				System.out.print(new String(familyArray,current.getFamilyOffset(),current.getFamilyLength()));
				System.out.print(":"+ new String(qualifierArray,current.getQualifierOffset(),current.getQualifierLength()));
				System.out.println(" "+new String(valueArray,current.getValueOffset(),current.getValueLength()));
			}
			System.out.println("---------------------------------------------");
		}
	}

	/**
	 * 过滤器
	 */
	@Test
	public void testFilter() throws Exception{
		// 前缀过滤器 PrefixFilter----针对行键
		// Filter filter = new PrefixFilter(Bytes.toBytes("user"));
		// testScan(filter);

		// 行键滤器 RowFilter---针对行健 （比较运算符、包含）
		// RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("user02")));
		// testScan(rowFilter);
		// RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("user"));
		// testScan(rowFilter);

		// 列值过滤器 SingleColumnValueExcludeFilter
		// SingleColumnValueFilter scvf = new SingleColumnValueExcludeFilter("base_info".getBytes(),"password".getBytes(), CompareFilter.CompareOp.EQUAL,"123456".getBytes());
		// 如果指定的列缺失，则也过滤掉
		// scvf.setFilterIfMissing(true);
		// testScan(scvf);

		// 列值过滤器 SingleColumnValueFilter 针对指定一个列的value的比较器来过滤
		// ByteArrayComparable comparable1 = new RegexStringComparator("^zhang");
		// ByteArrayComparable comparable2 = new RegexStringComparator("^ang");
		// SingleColumnValueFilter scvf = new SingleColumnValueExcludeFilter("base_info".getBytes(),"username".getBytes(), CompareFilter.CompareOp.EQUAL,comparable2);
		// testScan(scvf);

		// 列族过滤器 FamilyFilter 针对列族名的过滤器 返回结果中会包含满足条件的列族中的数据
		// FamilyFilter ff1 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("inf")));
		// FamilyFilter ff2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("base")));
		// testScan(ff2);

		// 列名过滤器 QualifierFilter 返回结果中只会包含满足条件的列的数据
		// QualifierFilter df1 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("password")));
		// QualifierFilter df2 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("us")));
		// testScan(df2);

		// 列名前缀过滤器 ColumnPrefixFilter
		// ColumnPrefixFilter cf = new ColumnPrefixFilter("passw".getBytes());
		// testScan(cf);

		// 列名前缀过滤器
		// byte[][] prefixes = new byte[][] {Bytes.toBytes("username"),Bytes.toBytes("password")};
		// MultipleColumnPrefixFilter mcf = new MultipleColumnPrefixFilter(prefixes);
		// testScan(mcf);

		// 过滤器列表 FilterList
		FamilyFilter ff2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL,new BinaryPrefixComparator(Bytes.toBytes("base")));
		ColumnPrefixFilter cf = new ColumnPrefixFilter("password".getBytes());
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filterList.addFilter(ff2);
		filterList.addFilter(cf);
		testScan(filterList);
	}

	public  void testScan(Filter filter) throws Exception{
		Table table = conn.getTable(TableName.valueOf("t_user_info"));
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner scanner =  table.getScanner(scan);
		Iterator<Result> iterable = scanner.iterator();
		while(iterable.hasNext()){
			Result result = iterable.next();
			CellScanner cellScanner = result.cellScanner();
			while(cellScanner.advance()){
				Cell current = cellScanner.current();
				byte[] familyArray = current.getFamilyArray();
				byte[] valueArray = current.getValueArray();
				byte[] qualifierArray = current.getQualifierArray();
				byte[] rowArray = current.getRowArray();

				System.out.println(new String(rowArray,current.getRowOffset(),current.getRowLength()));
				System.out.print(new String(familyArray,current.getFamilyOffset(),current.getFamilyLength()));
				System.out.print(":"+ new String(qualifierArray,current.getQualifierOffset(),current.getQualifierLength()));
				System.out.println(" "+new String(valueArray,current.getValueOffset(),current.getValueLength()));
			}
			System.out.println("---------------------------------------------");
		}
	}
}
