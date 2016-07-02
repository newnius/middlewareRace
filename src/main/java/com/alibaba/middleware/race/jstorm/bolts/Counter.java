package com.alibaba.middleware.race.jstorm.bolts;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class Counter implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5375137815288276938L;
	private OutputCollector collector;
	private Map<Long, Double> TBcounters;
	private Map<Long, Double> TMcounters;
	private Map<Long, Double> PCcounters;
	private Map<Long, Double> Mcounters;
	private static Logger LOG = LoggerFactory.getLogger(Counter.class);

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		TBcounters = new HashMap<>();
		TMcounters = new HashMap<>();
		PCcounters = new HashMap<>();
		Mcounters = new HashMap<>();

	}

	@Override
	public void execute(Tuple tuple) {
		int platform = (Integer)tuple.getValueByField("platform");
		long time = (Long) tuple.getValueByField("minuteTime");
		PaymentMessage payment = (PaymentMessage) tuple.getValueByField("payment");
		Double currentSum = 0.0;
		
		if(platform==OrderMessage.TAOBAO){
			if (!TBcounters.containsKey(time)) {
				currentSum = TBcounters.get(time);
			}
			Double sum = currentSum + payment.getPayAmount();
			TBcounters.put(time, sum);
			LOG.info("Total tb fee in " + time + ":" + sum);
		}else{
			if (!TMcounters.containsKey(time)) {
				currentSum = TMcounters.get(time);
			}
			Double sum = currentSum + payment.getPayAmount();
			TMcounters.put(time, sum);
			LOG.info("Total tm fee in " + time + ":" + sum);
		}
		
		if(payment.getPayPlatform() == PaymentMessage.PC){
			if (!PCcounters.containsKey(time)) {
				currentSum = PCcounters.get(time);
			}
			Double sum = currentSum + payment.getPayAmount();
			PCcounters.put(time, sum);
			LOG.info("Total pc fee in " + time + ":" + sum);
		}else{
			if (!Mcounters.containsKey(time)) {
				currentSum = Mcounters.get(time);
			}
			Double sum = currentSum + payment.getPayAmount();
			Mcounters.put(time, sum);
			LOG.info("Total mobile fee in " + time + ":" + sum);
		}
		
		collector.ack(tuple);
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(""));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
