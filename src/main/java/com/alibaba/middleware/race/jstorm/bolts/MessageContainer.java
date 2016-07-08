package com.alibaba.middleware.race.jstorm.bolts;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MessageContainer implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -977945595979586860L;
	private OutputCollector collector;
	private static Logger LOG = LoggerFactory.getLogger(MessageContainer.class);
	private HashMap<Long, Boolean> orders;
	private LinkedList<PaymentMessage> payments;
	private long startTime;
	// private Map<Long, Payment> waitingPayments;

	// private long freezeTime;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.orders = new HashMap<>();
		this.payments = new LinkedList<>();
		// this.waitingPayments = new HashMap<>();
		this.startTime = System.currentTimeMillis();
	}

	@Override
	public void execute(Tuple input) {
		String streamId = input.getSourceStreamId();

		OrderMessage order;
		PaymentMessage payment;
		switch (streamId) {
		case "tb-order":
			order = (OrderMessage) input.getValueByField("order");
			orders.put(order.getOrderId(), true);
			//LOG.info("Add TB order " + order.getOrderId());
			break;
		case "tm-order":
			order = (OrderMessage) input.getValueByField("order");
			orders.put(order.getOrderId(), false);
			//LOG.info("Add TM order " + order.getOrderId());
			break;
		case "payment":
			payment = (PaymentMessage) input.getValueByField("payment");
			//LOG.info("Add payment " + payment.getOrderId());
			if (orders.containsKey(payment.getOrderId())) {
				collector.emit("payment",
						new Values(payment, orders.get(payment.getOrderId()) ? Order.TAOBAO : Order.TMALL,
								RaceUtils.toMinuteTimestamp(payment.getCreateTime())));
				// LOG.info("emit imme " + "pay_" + payment.getOrderId());
			} else {
				payments.offer(payment);
			}

			break;
		case "flush":
			flush();
			
			break;
		default:
			LOG.warn("no such stream " + streamId);

		}
		collector.ack(input);

	}

	public void flush() {
		PaymentMessage payment;
		boolean hasEmited = false;
		do {
			payment = payments.peek();
			if (payment != null && orders.containsKey(payment.getOrderId())) {
				// waitingPayments.put(order, payment);
				payments.poll();
				collector.emit("payment",
						new Values(payment, orders.get(payment.getOrderId()) ? Order.TAOBAO : Order.TMALL,
								RaceUtils.toMinuteTimestamp(payment.getCreateTime())));
				//LOG.info("emit " + "pay_" + payment.getOrderId());
				//LOG.info("after emit, Total payments : " + payments.size());
				hasEmited = true;
			}
		} while (payment != null);

		if (!hasEmited || System.currentTimeMillis() - startTime > 15 * 60 * 1000) {
			collector.emit("flush", new Values());
			//LOG.info("Send flush stream");
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("payment", new Fields("payment", "platform", "minuteTime"));
		declarer.declareStream("flush", new Fields());
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
