package iot.example.apachestorm;
import java.util.ArrayList;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.JSONException;
import org.json.JSONObject;

import br.org.cesar.knot.lib.Credentials;
import br.org.cesar.knot.lib.KnotOperation;
import br.org.cesar.knot.lib.KnotService;
import io.socket.client.Socket;
import io.socket.emitter.Emitter;

public class HydrometerSpout implements IRichSpout {
	
	final String KNOT_SERVER_NAME = "knot-test.cesar.org.br";
	final int KNOT_PORT = 3000;
	
	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;
	
	private ArrayList<String> dataList = new ArrayList<>();

	@Override
	public void open(Map arg0, TopologyContext context, SpoutOutputCollector arg2) {
		this.collector = arg2;
		
		KnotService knot = new KnotOperation();
		
		Credentials credentials = new Credentials(
				"e121f259-3b4c-4afc-8f41-34496e4f0000",
				"0b8c4b2c8c18a6993a358185bc17d53b4e0557f2"
		);
		
		Socket connection = knot.connect(KNOT_SERVER_NAME, KNOT_PORT, credentials);
		
		knot.subscribe(connection, "e121f259-3b4c-4afc-8f41-34496e4f0000", new Emitter.Listener() {
			
			@Override
			public void call(Object... args) {
				JSONObject json = (JSONObject) args[0];

				try {
					if (json.has("message")) {
						String flowRate = json.getJSONObject("message").getString("flowRate");
						dataList.add(flowRate);
					}
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
		});
	}


	@Override
	public void nextTuple() {
		
		if(dataList.size() > 0) {
			this.collector.emit(new Values(dataList.get(dataList.size()-1)));
		}
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("flowRate"));
	}
	
	@Override
	public void ack(Object arg0) {
		System.err.println("ack()");
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
