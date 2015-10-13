package cassandra.test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TFramedTransportFactory;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ThriftDemo {
	private TTransport tr=null;  
    private Cassandra.Client client=null;  
    
    public ThriftDemo(String host,int port) throws TTransportException{
    	init(host,port);
    }
    public ThriftDemo(){
    	init("127.0.0.1",9160);
    }
	private void init(String host, int port) {
		// TODO Auto-generated method stub
//    	this.tr=new TFramedTransportFactory().openTransport(host, port);
    	this.tr=new TFramedTransport(new TSocket(host, port));
    	TProtocol proto = new TBinaryProtocol(tr);  
        this.client = new Cassandra.Client(proto);  
	}
	 public void open() throws Exception {  
	        tr.open();  
	        if (!tr.isOpen()) {  
	            throw new Exception("connect failed");  
	        }  
	    }  
	 public void close() {  
	        tr.close();  
	    }  
	 public void setKeySpace(String keyspace) throws Exception {  
	        client.set_keyspace(keyspace);// 使用myKeyspace keyspace  
	    }  
	 /** 
	     * 插入n条数据 
	     * @throws Exception 
	     */  
	    public void insert(String columnFamily,int n) throws Exception {  
	        ColumnParent parent = new ColumnParent(columnFamily);// column family  
	  
	        for (int i = 0; i < n; i++) {  
	            long timestamp = System.currentTimeMillis();// 时间戳  
	            ByteBuffer columnKey=toByteBuffer(i+"");
	            
	            Column nameColumn = new Column(toByteBuffer("name"));  
	            nameColumn.setValue(toByteBuffer("name" + i));  
	            nameColumn.setTimestamp(timestamp);  
	            client.insert(columnKey, parent, nameColumn,ConsistencyLevel.ONE);  
	  
	            Column ageColumn = new Column(toByteBuffer("age"));  
	            ageColumn.setValue(toByteBuffer(i * 2 + ""));  
	            ageColumn.setTimestamp(timestamp);  
	            client.insert(columnKey, parent, ageColumn, ConsistencyLevel.ONE);  
	            
	            Column meailColumn=new Column(toByteBuffer("email"));
	            meailColumn.setValue(toByteBuffer("123456789@qq.com"));
	            meailColumn.setTimestamp(timestamp);
	            client.insert(columnKey, parent, meailColumn, ConsistencyLevel.ONE);
	        }  
	    }
	    
	    /** 
	     * 查询一个列的值 
	     * @param key 
	     * @param columnName 
	     * @param columnFamily 
	     * @throws Exception 
	     */  
	    public void findOneColumn(String key, String columnName, String columnFamily) throws Exception {  
	        ColumnPath path = new ColumnPath(columnFamily);   
	        path.setColumn(toByteBuffer(columnName)); // 读取id  
	  
	        ColumnOrSuperColumn column=new ColumnOrSuperColumn();
			try {
				column = client.get(toByteBuffer(key), path, ConsistencyLevel.ONE);
				System.out.println(toString(column.column.name) + "->" + toString(column.column.value));  
			} catch (InvalidRequestException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnavailableException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TimedOutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
	    } 
	    /** 
	     * 查询一条数据所有列的值 
	     * @param key 
	     * @param columnFamily 
	     * @throws Exception 
	     */  
	    public void findAllColumn(String key, String columnFamily) throws Exception {  
	        ColumnParent parent = new ColumnParent(columnFamily);// column family  
	  
	        SlicePredicate predicate = new SlicePredicate();  
	        SliceRange sliceRange = new SliceRange(toByteBuffer(""),  
	                toByteBuffer(""), false, 10);  
	        predicate.setSlice_range(sliceRange);  
	        List<ColumnOrSuperColumn> results = client.get_slice(toByteBuffer(key),  
	                parent, predicate, ConsistencyLevel.ONE);  
	  
	        for (ColumnOrSuperColumn result : results) {  
	            System.out.print("{" + toString(result.column.name) + " -> "  
	                    + toString(result.column.value) + "}  ");  
	        }  
	        System.out.println();  
	    }  
	    /** 
	     * 查询多条数据 
	     * @param columnFamily 
	     * @throws Exception 
	     */  
	    public void findMulti(String columnFamily) throws Exception {  
	        ColumnParent parent = new ColumnParent(columnFamily);// column family  
	  
	        SlicePredicate predicate = new SlicePredicate();  
	        SliceRange sliceRange = new SliceRange();  
	        sliceRange.setStart("".getBytes());  
	        sliceRange.setFinish("".getBytes());  
	          
	        predicate.setSlice_range(sliceRange);  
	  
	        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();  
	        keys.add(toByteBuffer("1"));  
	        keys.add(toByteBuffer("3"));  
	        keys.add(toByteBuffer("6"));  
	  
	        Map<ByteBuffer, List<ColumnOrSuperColumn>> multiMap = client.multiget_slice(keys, parent, predicate, ConsistencyLevel.ONE);  
	  
	        for (Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry : multiMap .entrySet()) {  
	            System.out.print("key=" + toString(entry.getKey()) + "  ");  
	            List<ColumnOrSuperColumn> value = entry.getValue();  
	            for (ColumnOrSuperColumn column : value) {  
	                System.out.print("{" + toString(column.column.name) + " -> " + toString(column.column.value) + "}  ");  
	            }  
	            System.out.println();  
	        }  
	    }  
	    /** 
	     * 使用KeyRange查询 
	     * @param columnFamily 
	     * @throws Exception 
	     */  
	    public void findRange(String columnFamily)throws Exception{  
	        ColumnParent parent = new ColumnParent(columnFamily);// column family  
	        KeyRange keyRange = new KeyRange();  
	        keyRange.setStart_key("".getBytes());    
	        keyRange.setEnd_key("".getBytes());     
	          
	        SlicePredicate predicate = new SlicePredicate();    
//	        List<ByteBuffer> column_names = new ArrayList<ByteBuffer>();    
//	        column_names.add(toByteBuffer("age"));    
//	        column_names.add(toByteBuffer("email"));  
//	        predicate.setColumn_names(column_names);  
	        SliceRange sliceRang=new SliceRange(toByteBuffer(""), toByteBuffer(""), false, 10);
	       predicate.setSlice_range(sliceRang) ;
	        List<KeySlice> get_range_slices = client.get_range_slices(parent, predicate, keyRange, ConsistencyLevel.ONE);  
	        for(KeySlice keySlice : get_range_slices){  
	            System.out.print("key=" + toString(keySlice.key) + "  ");  
	            for (ColumnOrSuperColumn column : keySlice.columns) {  
	                System.out.print("{" + toString(column.column.name) + " -> " + toString(column.column.value) + "}  ");  
	            }  
	            System.out.println();  
	        }  
	    }  
	    /** 
	     * 删除 
	     * @param key 
	     * @param columnFamily 
	     * @throws Exception 
	     */  
	    public void remove(String key,String columnFamily)throws Exception{  
	        ColumnPath path = new ColumnPath(columnFamily);   
	        long temp = System.currentTimeMillis();  
	        client.remove(toByteBuffer(key), path, temp, ConsistencyLevel.ONE);  
	    }  
	    public void remove(String key,String columnFamily,String column)throws Exception{  
	        ColumnPath path = new ColumnPath(columnFamily);
	        path.setColumn(toByteBuffer(column));
	        long temp = System.currentTimeMillis();  
	        client.remove(toByteBuffer(key), path, temp, ConsistencyLevel.ONE);  
	    }  
	    /** 
	     * 更新 
	     * @param key 
	     * @param columnName 
	     * @param columnFamily 
	     * @param value 
	     * @throws Exception 
	     */  
	    public void update(String key,String columnFamily,String columnName,String value)throws Exception{  
	        ColumnParent parent = new ColumnParent(columnFamily);// column family  
	          
	        long timestamp = System.currentTimeMillis();// 时间戳  
	        Column column = new Column(toByteBuffer(columnName));  
	        column.setValue(toByteBuffer(value));  
	        column.setTimestamp(timestamp);  
	        ByteBuffer ageColumnKey = toByteBuffer(key);  
	        client.insert(ageColumnKey, parent, column, ConsistencyLevel.ONE);  
	    }  
	private static ByteBuffer toByteBuffer(String value) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		return  ByteBuffer.wrap(value.getBytes("UTF-8")); 
	}  
	 private static String toString(ByteBuffer buffer) throws Exception {  
	        byte[] bytes = new byte[buffer.remaining()];
	        buffer.get(bytes);  
	        return new String(bytes, "UTF-8");  
	    }  
}
