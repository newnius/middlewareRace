package com.alibaba.middleware.race.jstorm.bolts;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.Payment;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class OrderGetter implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9059379391786693480L;
	private OutputCollector collector;
	private TairOperatorImpl tair;
	private static Logger logger = LoggerFactory.getLogger(OrderGetter.class);

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		tair = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup,
				RaceConfig.TairNamespace);
	}

	@Override
	public void execute(Tuple tuple) {
		Payment payment = (Payment) tuple.getValueByField("payment");
		
		logger.info(payment.toString());
		
		Object value = tair.get(RaceConfig.prex_order + payment.getOrderId());
		if (value == null) {
			collector.fail(tuple);
			logger.warn(RaceConfig.prex_order + payment.getOrderId() + " is null");
		} else {
			Integer platform = (Integer) value;

			collector.emit(new Values(platform, payment, RaceUtils.toMinuteTimestamp(payment.getCreateTime())));
			collector.ack(tuple);
		}
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("platform", "payment", "minuteTime"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
