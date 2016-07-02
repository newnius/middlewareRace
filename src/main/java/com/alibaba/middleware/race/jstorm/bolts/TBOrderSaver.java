package com.alibaba.middleware.race.jstorm.bolts;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TBOrderSaver implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6290957298762116810L;
	private static Logger logger = LoggerFactory.getLogger(TBOrderSaver.class);
	private OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		OrderMessage orderMessage = (OrderMessage) tuple.getValueByField("order");
		
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        
        //写入tair
        tairOperator.write(RaceConfig.prex_tmall + orderMessage.getOrderId(), orderMessage.getPlatform());
		logger.info("Write order " + orderMessage.getOrderId() +" into Tair.");
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
