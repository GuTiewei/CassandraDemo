package cassandra.test;

public class ThriftTest {

	private static String ks="test_ks";
	private static String kf="base_info";
	public static void main(String[]args) throws Exception{
		ThriftDemo thriftDemo = new ThriftDemo();
		thriftDemo.open();
		thriftDemo.setKeySpace(ks);
		thriftDemo.remove("1", kf,"name");
//		thriftDemo.insert(kf, 20);
//		thriftDemo.findOneColumn("1", "name", kf);
//		thriftDemo.update("6", kf, "job", "doctor");
//		thriftDemo.findRange(kf);
		thriftDemo.findMulti(kf);
		thriftDemo.close();
	}
}
