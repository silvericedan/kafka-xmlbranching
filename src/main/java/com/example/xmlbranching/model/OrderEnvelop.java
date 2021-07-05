package com.example.xmlbranching.model;

import com.example.model.Order;
import lombok.Data;

@Data
public class OrderEnvelop {

    String xmlOrderKey;
    String xmlOrderValue;

    String orderTag;
    Order ValidOrder;
}
