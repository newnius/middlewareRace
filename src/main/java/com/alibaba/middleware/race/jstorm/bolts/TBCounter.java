package com.alibaba.middleware.race.jstorm.bolts;

import java.util.Map;

import com.alibaba.middleware.race.model.OrderMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TBCounter implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5375137815288276938L;
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple tuple) {
        OrderMessage orderMessage = (OrderMessage)tuple.getValueByField("order");
        
		collector.ack(tuple);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
