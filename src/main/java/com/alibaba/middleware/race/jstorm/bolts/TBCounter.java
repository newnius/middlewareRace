package com.alibaba.middleware.race.jstorm.bolts;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.model.OrderMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TBCounter implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5375137815288276938L;
	private OutputCollector collector;
	private Map<Long, Double> counters;
	private static Logger LOG = LoggerFactory.getLogger(TBCounter.class);

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		counters = new HashMap<>();

	}

	@Override
	public void execute(Tuple tuple) {
		long time = (Long) tuple.getValueByField("order");
		OrderMessage orderMessage = (OrderMessage) tuple.getValueByField("order");
		Double currentSum = 0.0;
		if (!counters.containsKey(time)) {
			currentSum = counters.get(time);
		}
		Double sum = currentSum + orderMessage.getTotalPrice();
		counters.put(time, sum);
		LOG.info("Total fee in " + time + ":" + sum);
		
		collector.ack(tuple);
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}