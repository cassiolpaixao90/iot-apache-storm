package iot.example.apachestorm;
import java.util.ArrayList;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class CounterBolt implements IRichBolt{

	ArrayList<String> palavras = new ArrayList<>();
	ArrayList<Integer> contador = new ArrayList<>();
	
	private static final long serialVersionUID = 1L;

	@Override
	public void cleanup() {
		
	}

	@Override
	public void execute(Tuple input) {
		String palavra = input.getStringByField("palavra");
		System.out.println("execute");
		
		int idx = palavras.indexOf(palavra);
		if(idx != -1) {
			contador.set(idx, contador.get(idx)+1);
		}else {
			palavras.add(palavra);
			contador.add(1);
		}
		
		if( idx % 10 == 0) {
			System.out.println(palavras.toString());
			System.out.println(contador.toString());
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector outp) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}	

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
