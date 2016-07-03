package com.alibaba.middleware.race.jstorm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

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
import java.util.concurrent.LinkedBlockingQueue;

public class MqReader implements IRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7527748428170112375L;
	private DefaultMQPushConsumer consumer;
	private LinkedBlockingQueue<Order> TMorders;
	private LinkedBlockingQueue<Order> TBorders;
	private LinkedBlockingQueue<Payment> payments;
	private long startTime;

	private static Logger LOG = LoggerFactory.getLogger(MqReader.class);
	private SpoutOutputCollector _collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		TBorders = new LinkedBlockingQueue<>();
		TMorders = new LinkedBlockingQueue<>();
		payments = new LinkedBlockingQueue<>();
		startTime = System.currentTimeMillis();
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

					LOG.info(msg.getTopic());
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
						LOG.info("TBOrderId:" + order.getOrderId());
						LOG.info(order.toString());
						try {
							TBorders.put(order);
							LOG.info("after put, Total TBorders : " + TBorders.size());
						} catch (InterruptedException e) {
							e.printStackTrace();
							LOG.error(e.getMessage());
						}
						break;

					case RaceConfig.MqTmallTradeTopic:
						if (body.length == 2 && body[0] == 0 && body[1] == 0) {
							// Info: 生产者停止生成数据, 并不意味着马上结束
							LOG.info("Got the end signal of TBOrderMessage");
							continue;
						}

						orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
						order = new Order(orderMessage);
						order.setPlatform(Order.TMALL);
						LOG.info("TMOrderId:" + order.getOrderId());
						LOG.info(order.toString());
						try {
							TMorders.put(order);
							LOG.info("after put, Total TMorders : " + TMorders.size());
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
						LOG.info("payment OrderId:" + payment.getOrderId());
						LOG.info(payment.toString());
						try {
							payments.put(payment);
							LOG.info("after put, Total paymentss : " + payments.size());
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

		Order order = null;
		Payment payment;

		// emit tb orders
		do {
			order = TBorders.poll();
			if (order != null) {
				_collector.emit("tb-order", new Values(order));
			} else {
				Utils.sleep(20);
			}
		} while (order != null);

		// emit tm orders
		do {
			order = TMorders.poll();
			if (order != null) {
				_collector.emit("tm-order", new Values(order));
			} else {
				Utils.sleep(20);
			}
		} while (order != null);

		//start emit after 60s
		if (System.currentTimeMillis() - startTime > 1000 * 60) {
			// emit payments
			do {
				payment = payments.poll();
				if (payment != null) {
					_collector.emit("payment", new Values(payment));
				} else {
					Utils.sleep(20);
				}
			} while (payment != null);
		}
	}

	@Override
	public void ack(Object id) {
		LOG.info("ack " + id);
	}

	@Override
	public void fail(Object id) {
		LOG.info("fail " + id);
		_collector.emit(new Values(id), id);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("tb-order", new Fields("order"));
		declarer.declareStream("tm-order", new Fields("order"));
		declarer.declareStream("payment", new Fields("payment"));
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