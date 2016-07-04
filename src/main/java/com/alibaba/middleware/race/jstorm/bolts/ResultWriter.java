package com.alibaba.middleware.race.jstorm.bolts;

import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class ResultWriter implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2030583843338570399L;
	private OutputCollector collector;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private TairOperatorImpl tair;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.tair = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
				RaceConfig.TairGroup, RaceConfig.TairNamespace);
	}

	@Override
	public void execute(Tuple tuple) {
		Serializable key = (Serializable) tuple.getValueByField("key");
		Double value = (Double) tuple.getValueByField("value");

		// 写入tair
		tair.incrBy(key, value);
		//logger.info("Write result {" + key + ":" + value + "} into Tair.");

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
