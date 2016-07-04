package com.alibaba.middleware.race.model;

import java.io.Serializable;

/**
 * 我们后台RocketMq存储的订单消息模型类似于OrderMessage，选手也可以自定义
 * 订单消息模型，只要模型中各个字段的类型和顺序和OrderMessage一样，即可用Kryo
 * 反序列出消息
 */
public class Order implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 2337105241957909006L;
	private long orderId; //订单ID
    private String buyerId; //买家ID
    private String productId; //商品ID

    private String salerId; //卖家ID
    private long createTime; //13位数数，毫秒级时间戳，订单创建时间
    private double totalPrice;
    
    private int platform;
    
    public static final int TAOBAO = 0;
    public static final int TMALL = 1;


    //Kryo默认需要无参数构造函数
    @SuppressWarnings("unused")
	private Order() {

    }
	
	
	public Order(OrderMessage orderMessage){
		super();
		this.orderId = orderMessage.getOrderId();
		this.createTime = orderMessage.getCreateTime();
		this.buyerId = orderMessage.getBuyerId();
		this.productId = orderMessage.getProductId();
		this.salerId = orderMessage.getSalerId();
		this.totalPrice = orderMessage.getTotalPrice();
	}



	@Override
    public String toString() {
        return "OrderMessage{" +
                "orderId=" + orderId +
                ", buyerId='" + buyerId + '\'' +
                ", productId='" + productId + '\'' +
                ", salerId='" + salerId + '\'' +
                ", createTime=" + createTime +
                ", totalPrice=" + totalPrice +
                ", platform="+ platform +
                '}';
    }

    public double getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(double totalPrice) {
        this.totalPrice = totalPrice;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public String getBuyerId() {
        return buyerId;
    }

    public void setBuyerId(String buyerId) {
        this.buyerId = buyerId;
    }


    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }


    public String getSalerId() {
        return salerId;
    }

    public void setSalerId(String salerId) {
        this.salerId = salerId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

	public int getPlatform() {
		return platform;
	}

	public void setPlatform(int platform) {
		this.platform = platform;
	}
    
    

}
