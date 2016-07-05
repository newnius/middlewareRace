package com.alibaba.middleware.race.jstorm.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.model.Payment;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

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
	private long startTime;
	private boolean updateToDate = true;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		TBcounters = new TreeMap<>();
		TMcounters = new TreeMap<>();
		PCcounters = new TreeMap<>();
		Mcounters = new TreeMap<>();
		startTime = System.currentTimeMillis();
	}

	@Override
	public void execute(Tuple tuple) {
		if ("flush".equals(tuple.getSourceStreamId())) {
			// LOG.info("Got flush signal");
			if (!updateToDate) {
				flush();
			}
			updateToDate = true;
		} else {

			int platform = (Integer) tuple.getValueByField("platform");
			long time = (Long) tuple.getValueByField("minuteTime");
			Payment payment = (Payment) tuple.getValueByField("payment");
			double currentSum = 0.0;

			// LOG.info(payment.toString());

			if (platform == Order.TAOBAO) {
				if (TBcounters.containsKey(time)) {
					currentSum = TBcounters.get(time);
				}
				Double sum = currentSum + payment.getPayAmount();
				TBcounters.put(time, sum);
				// collector.emit(new Values(RaceConfig.prex_taobao + time,
				// sum));
				// LOG.info("Total tb fee in " + time + ":" + sum);
			} else {
				if (TMcounters.containsKey(time)) {
					currentSum = TMcounters.get(time);
				}
				Double sum = currentSum + payment.getPayAmount();
				TMcounters.put(time, sum);
				// collector.emit(new Values(RaceConfig.prex_tmall + time,
				// sum));
				// LOG.info("Total tm fee in " + time + ":" + sum);
			}

			currentSum = 0.0;
			if (payment.getPayPlatform() == Payment.PC) {
				if (PCcounters.containsKey(time)) {
					currentSum = PCcounters.get(time);
				}
				Double sum = currentSum + payment.getPayAmount();
				PCcounters.put(time, sum);
				// LOG.info("Total pc fee in " + time + ":" + sum);
			} else {
				if (Mcounters.containsKey(time)) {
					currentSum = Mcounters.get(time);
				}
				Double sum = currentSum + payment.getPayAmount();
				Mcounters.put(time, sum);
				// LOG.info("Total mobile fee in " + time + ":" + sum);
			}
			updateToDate = false;
			if (System.currentTimeMillis() - startTime > 16 * 60 * 1000) {
				flush();
			}
		}

		collector.ack(tuple);
	}

	public void flush() {
		// emit to save
		Set<Long> minuteTimes = TBcounters.keySet();
		List<Long> toBeDelete = new ArrayList<>();
		for (Long minuteTime : minuteTimes) {
			collector.emit(new Values(RaceConfig.prex_taobao + minuteTime, TBcounters.get(minuteTime)));
			toBeDelete.add(minuteTime);
		}
		for (Long minuteTime : toBeDelete) {
			TBcounters.remove(minuteTime);
		}

		minuteTimes = TMcounters.keySet();
		toBeDelete.clear();
		for (Long minuteTime : minuteTimes) {
			collector.emit(new Values(RaceConfig.prex_tmall + minuteTime, TMcounters.get(minuteTime)));
			toBeDelete.add(minuteTime);
		}
		for (Long minuteTime : toBeDelete) {
			TMcounters.remove(minuteTime);
		}

		Double totalPC = 0.0;
		Double totalWire = 0.0;
		for (Long minuteTime : PCcounters.keySet()) {
			if (PCcounters.containsKey(minuteTime)) {
				totalPC += PCcounters.get(minuteTime);
			}
			if (Mcounters.containsKey(minuteTime)) {
				totalWire += Mcounters.get(minuteTime);
			}
			// avoid overflow exception
			if (Math.abs(totalPC - 0.0) < 0.001) {
				totalPC = 1.0;
			}
			collector.emit(new Values(RaceConfig.prex_ratio + minuteTime, totalWire / totalPC));
		}

	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "value"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
