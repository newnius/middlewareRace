package com.alibaba.middleware.race.model;

import java.io.Serializable;


/**
 * 我们后台RocketMq存储的交易消息模型类似于PaymentMessage，选手也可以自定义
 * 订单消息模型，只要模型中各个字段的类型和顺序和PaymentMessage一样，即可用Kryo
 * 反序列出消息
 */

public class Payment implements Serializable{


    /**
	 * 
	 */
	private static final long serialVersionUID = -3428231505051095311L;

	private long orderId; //订单ID

    private double payAmount; //金额

    /**
     * Money来源
     * 0,支付宝
     * 1,红包或代金券
     * 2,银联
     * 3,其他
     */
    private short paySource; //来源

    /**
     * 支付平台
     * 0，pC
     * 1，无线
     */
    private short payPlatform; //支付平台
    
    
    public static final int PC = 0;
    public static final int MOBILE = 1;

    /**
     * 付款记录创建时间
     */
    private long createTime; //13位数，毫秒级时间戳，初赛要求的时间都是指该时间

    //Kryo默认需要无参数构造函数
    public Payment() {
    }
    
    


    public Payment(PaymentMessage paymentMessage) {
		super();
		this.orderId = paymentMessage.getOrderId();
		this.payAmount = paymentMessage.getPayAmount();
		this.paySource = paymentMessage.getPaySource();
		this.payPlatform = paymentMessage.getPayPlatform();
		this.createTime = paymentMessage.getCreateTime();
	}




	@Override
    public String toString() {
        return "PaymentMessage{" +
                "orderId=" + orderId +
                ", payAmount=" + payAmount +
                ", paySource=" + paySource +
                ", payPlatform=" + payPlatform +
                ", createTime=" + createTime +
                '}';
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public double getPayAmount() {
        return payAmount;
    }

    public void setPayAmount(double payAmount) {
        this.payAmount = payAmount;
    }

    public short getPaySource() {
        return paySource;
    }

    public void setPaySource(short paySource) {
        this.paySource = paySource;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public short getPayPlatform() {
        return payPlatform;
    }
}
