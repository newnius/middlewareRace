package com.alibaba.middleware.race.jstorm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.Payment;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class MqReader implements IRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7527748428170112375L;
	private DefaultMQPushConsumer consumer;
	private LinkedBlockingQueue<Order> TMOrders;
	private Map<Long, Order> waitingTMOrders;
	private LinkedBlockingQueue<Order> TBOrders;
	private Map<Long, Order> waitingTBOrders;
	private LinkedBlockingQueue<Payment> payments;
	private Map<Long, Payment> waitingPayments;
	private long startTime;
	private long freezeTime;
	private AtomicLong msgIdGenerator = new AtomicLong(1);

	private static Logger LOG = LoggerFactory.getLogger(MqReader.class);
	private SpoutOutputCollector _collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		TBOrders = new LinkedBlockingQueue<>();
		waitingTBOrders = new ConcurrentHashMap<>();
		TMOrders = new LinkedBlockingQueue<>();
		waitingTMOrders = new ConcurrentHashMap<>();
		payments = new LinkedBlockingQueue<>();
		waitingPayments = new ConcurrentHashMap<>();
		startTime = System.currentTimeMillis();
		freezeTime = System.currentTimeMillis();
		consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		//
		if (RaceConfig.RocketMqAddr.length() > 0)
			consumer.setNamesrvAddr(RaceConfig.RocketMqAddr);
		try {
			consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqPayTopic, "*");
		} catch (MQClientException ex) {
			ex.printStackTrace();
			LOG.error(ex.getErrorMessage());
		}

		consumer.registerMessageListener(new MessageListenerConcurrently() {

			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				for (MessageExt msg : msgs) {

					// LOG.info(msg.getTopic());
					byte[] body = msg.getBody();
					OrderMessage orderMessage;
					Order order;

					switch (msg.getTopic()) {
					case RaceConfig.MqTaobaoTradeTopic:

						if (body.length == 2 && body[0] == 0 && body[1] == 0) {
							// Info: 生产者停止生成数据, 并不意味着马上结束
							LOG.info("Got the end signal of TBOrderMessage");
							continue;
						}

						orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
						order = new Order(orderMessage);
						order.setPlatform(Order.TAOBAO);
						// LOG.info("TBOrderId:" + order.getOrderId());
						// LOG.info(order.toString());
						try {
							TBOrders.put(order);
							// LOG.info("after put, Total TBorders : " +
							// TBOrders.size());
						} catch (InterruptedException e) {
							e.printStackTrace();
							LOG.error(e.getMessage());
						}
						break;

					case RaceConfig.MqTmallTradeTopic:
						if (body.length == 2 && body[0] == 0 && body[1] == 0) {
							// Info: 生产者停止生成数据, 并不意味着马上结束
							LOG.info("Got the end signal of TMOrderMessage");
							continue;
						}

						orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
						order = new Order(orderMessage);
						order.setPlatform(Order.TMALL);
						// LOG.info("TMOrderId:" + order.getOrderId());
						// LOG.info(order.toString());
						try {
							TMOrders.put(order);
							// LOG.info("after put, Total TMorders : " +
							// TMOrders.size());
						} catch (InterruptedException e) {
							e.printStackTrace();
							LOG.error(e.getMessage());
						}
						break;

					case RaceConfig.MqPayTopic:
						if (body.length == 2 && body[0] == 0 && body[1] == 0) {
							// Info: 生产者停止生成数据, 并不意味着马上结束
							LOG.info("Got the end signal of payments");
							continue;
						}
						PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
						Payment payment = new Payment(paymentMessage);
						// LOG.info("payment OrderId:" + payment.getOrderId());
						// LOG.info(payment.toString());
						try {
							payments.put(payment);
							// LOG.info("after put, Total payments : " +
							// payments.size());
						} catch (InterruptedException e) {
							e.printStackTrace();
							LOG.error(e.getMessage());
						}
						break;
					}

				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

		try {
			consumer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
			LOG.error(e.getErrorMessage());
		}

	}

	@Override
	public void nextTuple() {
		// LOG.info("before get, Total orders : " + orders.size());
		boolean hasEmited = false;
		Order order = null;
		Payment payment;

		// emit tb orders
		do {
			order = TBOrders.poll();
			if (order != null) {
				long msgId = msgIdGenerator.getAndIncrement();
				waitingTBOrders.put(msgId, order);
				_collector.emit("tb-order", new Values(order), "tb_" + msgId);
				hasEmited = true;
			}
		} while (order != null);

		// emit tm orders
		do {
			order = TMOrders.poll();
			if (order != null) {
				long msgId = msgIdGenerator.getAndIncrement();
				waitingTMOrders.put(msgId, order);
				_collector.emit("tm-order", new Values(order), "tm_" + msgId);
				hasEmited = true;
			}
		} while (order != null);

		// start emit after 60s
		if (System.currentTimeMillis() > freezeTime) {
			// emit payments
			do {
				payment = payments.poll();
				if (payment != null) {
					long msgId = msgIdGenerator.getAndIncrement();
					waitingPayments.put(msgId, payment);
					_collector.emit("payment", new Values(payment), "pay_" + msgId);
					hasEmited = true;
				}
			} while (payment != null);
		}
		if (!hasEmited && System.currentTimeMillis() - startTime > 3 * 60 * 1000) {
			_collector.emit("flush", new Values());
			// LOG.info("Send flush stream");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void ack(Object id) {
		String messageId = (String) id;
		String[] aStrings = messageId.split("_");
		String type = aStrings[0];
		long msgId = new Long(aStrings[1]);
		switch (type) {
		case "tb":
			waitingTBOrders.remove(msgId);
			break;

		case "tm":
			waitingTMOrders.remove(msgId);
			break;

		case "pay":
			waitingPayments.remove(msgId);
			break;
		}
		LOG.info("ack " + id);
	}

	@Override
	public void fail(Object id) {
		String messageId = (String) id;
		String[] aStrings = messageId.split("_");
		String type = aStrings[0];
		long msgId = new Long(aStrings[1]);
		switch (type) {
		case "tb":
			Order order = waitingTBOrders.get(msgId);
			if (order != null) {
				_collector.emit("tb-order", new Values(order), "tb_" + msgId);
			} else {
				LOG.warn(messageId + " is null");
			}
			break;

		case "tm":
			Order order2 = waitingTMOrders.get(msgId);
			if (order2 != null) {
				_collector.emit("tm-order", new Values(order2), "tm_" + msgId);
			} else {
				LOG.warn(messageId + " is null");
			}
			break;

		case "pay":
			Payment payment = waitingPayments.get(msgId);
			if (payment != null) {
				_collector.emit("payment", new Values(payment), "pay_" + msgId);
			} else {
				LOG.warn(messageId + " is null");
			}
			break;
		}
		freezeTime = System.currentTimeMillis() + 100;
		LOG.info("fail " + id);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("tb-order", new Fields("order"));
		declarer.declareStream("tm-order", new Fields("order"));
		declarer.declareStream("payment", new Fields("payment"));
		declarer.declareStream("flush", new Fields());
	}

	@Override
	public void close() {

	}

	@Override
	public void activate() {

	}

	@Override
	public void deactivate() {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}