package iot.example.apachestorm;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitterBolt implements IRichBolt {

	OutputCollector collector;
	private static final long serialVersionUID = 1L;

	@Override
	public void cleanup() { }
	
	public SplitterBolt() {	}

	@Override
	public void execute(Tuple tuple) {
		String frase = tuple.getStringByField("frase");
		String[] palavras = frase.split(" ");
		for (int i = 0; i < palavras.length; i++) {
			collector.emit(new Values(palavras[i]));
		}
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("palavra"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
