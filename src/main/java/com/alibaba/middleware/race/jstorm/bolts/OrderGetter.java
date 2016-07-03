package com.alibaba.middleware.race.jstorm.bolts;

import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentMessage;

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

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		tair = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup,
				RaceConfig.TairNamespace);
	}

	@Override
	public void execute(Tuple tuple) {
		PaymentMessage payment = (PaymentMessage) tuple.getValueByField("payment");
		Integer platform = (Integer) tair.get(RaceConfig.prex_order + payment.getOrderId());
		if (platform == null) {
			collector.fail(tuple);
		}
		collector.emit(new Values(platform, payment, RaceUtils.toMinuteTimestamp(payment.getCreateTime())));
		collector.ack(tuple);
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
