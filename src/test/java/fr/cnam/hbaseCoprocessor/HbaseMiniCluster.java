package fr.cnam.hbaseCoprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import fr.cnam.hbaseCoprocessor.CnamRegionCoprocessor;

public class HbaseMiniCluster {

	private static HBaseTestingUtility utility;
	byte[] CF = "CF".getBytes();
	byte[] QUALIFIER = "CQ-1".getBytes();
	private final static TableName tableName = TableName.valueOf("table");
	private final static byte[] FAMILY = Bytes.toBytes("cf");
	private static Table table;
	private final static int ServerNum = 3;
	private static MiniHBaseCluster cluster;

	public HbaseMiniCluster() {
		super();
	}

	public Configuration initCluster() {
		Configuration conf = new Configuration();
//		conf.addResource("hbase-site.xml");
		conf.set("hbase.master.info.port", "-1");
		conf.set("hbase.regionserver.info.port", "-1");
		conf.set("hbase.regionserver.info.port.auto", "true");
		conf.set("hbase.regionserver.safemode", "false");
		conf.set("zookeeper.recovery.retry.intervalmill", "100");
		return conf;
	}

	public void start() {
		try {
			utility = new HBaseTestingUtility();

//			utility.getConfiguration().set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, CnamWalCoprocessor.class.getName(), CnamWalCoprocessor.class.getName());
			utility.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
					CnamRegionCoprocessor.class.getName());
			utility.getConfiguration().writeXml(System.out);
			cluster = utility.startMiniCluster(1, ServerNum);
			Path hbaseRootDir = utility.getDFSCluster().getFileSystem().makeQualified(new Path("/hbase"));
			FSUtils.setRootDir(initCluster(), hbaseRootDir);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void initTable() {
		try {
			table = utility.createTable(tableName, FAMILY, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
			utility.waitTableAvailable(tableName, 1000);
			utility.loadTable(table, FAMILY);
		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}
	}

	public void ingestTable() {
		for (int i = 0; i < 25; i++) {
			Put put = new Put((i + "row").getBytes());
			put.addImmutable(FAMILY, "col".getBytes(), ("value12" + i).getBytes());
			try {
				table.put(put);
				Get g = new Get("1row".getBytes());
				Result r = table.get(g);
				byte[] value = r.getValue(FAMILY, "col".getBytes());
//				System.err.println(Bytes.toString(value));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
//    	for (int i = 0; i < ServerNum; i++) {
//    		HRegionServer server = cluster.getRegionServer(i);
//    		for (Region region : server.getRegions(tableName)) {
//    			System.err.println(region.getCompactionState());
//    		}
//    	}
	}

	public void stopCluster() {
		try {
			cluster.shutdown();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
