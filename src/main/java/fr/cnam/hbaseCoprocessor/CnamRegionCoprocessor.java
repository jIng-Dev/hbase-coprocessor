package fr.cnam.hbaseCoprocessor;

import java.util.Optional;

import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;

public class CnamRegionCoprocessor implements RegionCoprocessor{

	@Override
	public Optional<RegionObserver> getRegionObserver() {
		return Optional.of(new CnamRegionObserver());
	}

	
}
