package fr.cnam.hbaseCoprocessor;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import fr.cnam.hbase.command.KafkaSendCommand;

public class CnamRegionObserver implements RegionObserver {

	private static CompletableFuture<String> MY_FUTURE = new CompletableFuture<>();

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability)
			throws IOException {

//		if(!edit.isMetaEdit()&&c.getEnvironment().getRegionInfo().getTable().getNameAsString().equals("table")) {
//			System.err.println("GET REGION ROW EMPTY:"+edit.isEmpty()+ " | REPLAY |"+edit.isReplay()+" ||| "+new String(put.getRow())+" ||| "+edit.getRowForRegion(c.getEnvironment().getRegionInfo()).length);
		List<Cell> cells = put.get(Bytes.toBytes("cf"), "col".getBytes());
		for (Cell cell : cells) {
//				System.out.println("METAFAMILY:"+new String(cell.getFamilyArray())+" ___ "+new String(cell.getValueArray())+" ___ "+cell.getType().name()+" ___ ");
			MY_FUTURE.whenComplete((value, error) -> {
				if (error != null) {
					error.printStackTrace();
				}
			});
			Thread t = new Thread(() -> {
				try {
					TimeUnit.NANOSECONDS.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				MY_FUTURE.complete(KafkaSendCommand.getInstance().sendRecord(new String(cell.getValueArray())));
//							;new String(c.getRowArray()));
			});
			t.start();
//			}
		}
		RegionObserver.super.postPut(c, put, edit, durability);
	}

}
