package cassandra.test;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
public class CqlTest {

	/**
	 * @param args
	 */
	private static String keyspace="test_ks";
	private static String table="users";
	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub

		CqlDemo demo=new CqlDemo();
		
//		query(demo);
//		insertData(demo);
//		demo.update(keyspace, table);
//		demo.update(keyspace, table, "5eaa6698-d406-454d-bf38-91998f88cd8e");
		
//		deleteColumns(demo);
//		demo.deleteRow(keyspace, table,"5eaa6698-d406-454d-bf38-91998f88cd8e");
//		insertData2(demo);
//	demo.selectData(keyspace, table);
	}
	private static void deleteColumns(CqlDemo demo) {
		String[] cloumns=new String[]{"avatar","age","last_name"};
		demo.delete(keyspace, table, cloumns,"5eaa6698-d406-454d-bf38-91998f88cd8e");
		demo.close();
	}
	private static void query(CqlDemo demo) {
		String[] cloumns=new String[]{"last_name","address"};
		demo.selectDataByQueryBuilder(keyspace, table, cloumns);
		demo.close();
	}
	private static void insertData2(CqlDemo demo) {
		String keys[]=new String[]{"id","first_name","age","address"};
		Object[]values=new Object[]{UUID.randomUUID(),"gtw",23,"changzhou"};
		demo.insertData(keyspace, table, keys, values);
		demo.close();
	}
	private static void insertData(CqlDemo demo) {
		Map<String, Object> data=new HashMap<String, Object>();
		
		data.put("first_name", "delete");
		data.put("last_name", "Wei");
		data.put("age", 22);
		Set<String>emails=new HashSet<String>();
		emails.add("12345@qq.com");
		emails.add("789@qq.com");
		data.put("emails", emails);
		ByteBuffer avatar=ByteBuffer.allocate(8);
		avatar.put("avatar".getBytes());
		avatar.flip();
		data.put("avatar", avatar);
		demo.insetData(keyspace, table, data);
		demo.close();
	}

}
