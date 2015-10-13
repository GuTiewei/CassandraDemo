package cassandra.test;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

public class CqlDemo {

	private Cluster cluster=null;
	private Session session=null;
	
	public CqlDemo(String host,int port) throws UnknownHostException{
		connect(host,port);
	}
	public CqlDemo() throws UnknownHostException{
		connect("127.0.0.1",9042);
	}
	private void connect(String address,int port) throws UnknownHostException{
		cluster = Cluster.builder().addContactPoint(address).withPort(port).build();
		Metadata metaData = cluster.getMetadata();
		System.out.println("ClusterName:"+metaData.getClusterName());
		for(Host host:metaData.getAllHosts()){
			System.out.println(host.getDatacenter()+"="+host.getAddress()+"="+host.getRack());
		}
		session=cluster.connect();
	}
	
	public void close(){
		cluster.close();
	}
	public void insetData(String keyspace,String table,Map<String,Object>data){
		PreparedStatement statement = this.session.prepare("insert into "+keyspace+"."+table+"(id,first_name,last_name,age,emails,avatar) "+"values(?,?,?,?,?,?);");
		 session.execute(new BoundStatement(statement).bind(UUID.randomUUID(),data.get("first_name"),data.get("last_name"),data.get("age"),data.get("emails"),data.get("avatar")));
	}
	public void selectData(String keyspace,String table){
		PreparedStatement statement = this.session.prepare("select * from "+keyspace+"."+table+";");
		ResultSet rs = this.session.execute(statement.getQueryString());
		for(Row row:rs.all()){
			System.out.print(row.getUUID("id")+"="+row.getString("first_name")+"="+row.getString("last_name")+"="+row.getInt("age")+"={");
			for(String email:row.getSet("emails", String.class))
				System.out.print(email);
			System.out.println("}");
		}
	}
	
	public void insertData(String keyspace,String table,String[]keys,Object[]values){
		Insert insert = QueryBuilder.insertInto(keyspace, table).values(keys, values);
		this.session.execute(insert);
	}
	
	/*
	 *  先建立first_name索引
	 */
	public void selectDataByQueryBuilder(String keyspace,String table,String[]cloumns){
		Select select=null;
		if(null!=cloumns)
		select = QueryBuilder.select(cloumns).from(keyspace, table);
		else select = QueryBuilder.select().from(keyspace, table);
		Where where = select.where(QueryBuilder.eq("first_name", "Mr"));
		ResultSet rs = this.session.execute(where);
		for(Row row:rs.all()){
			String string="";
			if(null!=cloumns)
				for(String cloumn:cloumns){
					string+=cloumn+"="+row.getString(cloumn)+",";
				}
			else{
				Iterator<Definition> it = row.getColumnDefinitions().iterator();
				while(it.hasNext()){
					Definition de = it.next();
					System.out.print(de.getName()+",");
				}
			}
			System.out.println(string);
		}
	}
	
	public void update(String keyspace,String table,String id){
		com.datastax.driver.core.querybuilder.Update.Where where = QueryBuilder.update(keyspace, table).with(QueryBuilder.set("last_name", "update_last_name")).where(QueryBuilder.eq("id", UUID.fromString(id)));
		this.session.execute(where);
	}
	public void delete(String keyspace,String table,String[]cloumns,String id){
		  com.datastax.driver.core.querybuilder.Delete.Where where = QueryBuilder.delete(cloumns).from(keyspace, table).where(QueryBuilder.eq("id",UUID.fromString(id)));
		this.session.execute(where);
	}
	public void deleteRow(String keyspace,String table,String id){
		com.datastax.driver.core.querybuilder.Delete.Where where = QueryBuilder.delete().from(keyspace, table).where(QueryBuilder.eq("id", UUID.fromString(id)));
		this.session.execute(where);
	}
}
