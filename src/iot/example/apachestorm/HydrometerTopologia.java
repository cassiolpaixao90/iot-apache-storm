package iot.example.apachestorm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class HydrometerTopologia {
	
	public static void main(String[] args) {
		
		try {
			Config config = new Config();
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("hydrometer_spout", new HydrometerSpout());
			builder.setBolt("splitter_bolt", new SplitterBolt()).shuffleGrouping("hydrometer_spout");
			builder.setBolt("counter_bolt", new CounterBolt()).shuffleGrouping("splitter_bolt");
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("iot-cesar", config, builder.createTopology());
			
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

}
