package cn.leo.hbase.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseClient {
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		// createTable();
		// put();
		get();
	}

	public static Configuration getConfig() {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "hadoop05");
		return config;
	}

	public static void createTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(getConfig());

		TableName tn = TableName.valueOf("t_girl");
		HTableDescriptor tDesc = new HTableDescriptor(tn);

		HColumnDescriptor c1Desc = new HColumnDescriptor("base_info");
		c1Desc.setMaxVersions(3);

		HColumnDescriptor c2Desc = new HColumnDescriptor("ext_info");
		c2Desc.setMaxVersions(3);

		tDesc.addFamily(c1Desc);
		tDesc.addFamily(c2Desc);

		admin.createTable(tDesc);

		admin.close();
	}

	public static void put() throws IOException {
		HTable t = new HTable(getConfig(), "t_girl");

		Put put = new Put(Bytes.toBytes("r1"));
		put.add(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("ab"));
		put.add(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
		put.add(Bytes.toBytes("ext_info"), Bytes.toBytes("size"), Bytes.toBytes("32C"));

		t.put(put);

		t.close();
	}

	public static void get() throws IOException {
		HTable t = new HTable(getConfig(), "t_girl");

		Get get = new Get(Bytes.toBytes("r1"));

		Result rs = t.get(get);

		List<Cell> listCells = rs.listCells();
		for (Cell cell : listCells) {
			int familyLength = cell.getFamilyLength();
			int familyOffset = cell.getFamilyOffset();
			String family = Bytes.toStringBinary(cell.getFamilyArray(), familyOffset, familyLength);

			int qualifierOffset = cell.getQualifierOffset();
			int qualifierLength = cell.getQualifierLength();
			String key = Bytes.toString(cell.getQualifierArray(), qualifierOffset, qualifierLength);

			int valueOffset = cell.getValueOffset();
			int valueLength = cell.getValueLength();
			String value = Bytes.toString(cell.getValueArray(), valueOffset, valueLength);
			System.out.println(family + ":" + key + ":" + value);

		}
	}
}
