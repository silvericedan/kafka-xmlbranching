package com.example.xmlbranching.services;

import com.example.model.Order;
import com.example.xmlbranching.bindings.OrderListenerBinding;
import com.example.xmlbranching.configs.AppConstants;
import com.example.xmlbranching.configs.AppSerdes;
import com.example.xmlbranching.model.OrderEnvelop;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;

@Service
@Log4j2
@EnableBinding(OrderListenerBinding.class)
public class OrderListenerService {

    //This two lines are reading the error topic name config from application.yaml
    @Value("${application.configs.error.topic.name}")
    private String ERROR_TOPIC;

    @StreamListener("xml-input-channel") //We define the input channel for THIS listener
    @SendTo({"india-orders-channel", "abroad-orders-channel"}) //Then we define two output channels for THIS listener
    //We return an array of KStream, we will return two streams, and the Spring Framework will send
    //the first element of the array to india-orders and the second to abroad orders
    public KStream<String, Order>[] process(KStream<String, String> input) {

        input.foreach((k, v) -> log.info(String.format("Received XML Order Key: %s, Value: %s", k, v)));

        KStream<String, OrderEnvelop> orderEnvelopKStream = input.map((key, value) -> {
            OrderEnvelop orderEnvelop = new OrderEnvelop();
            orderEnvelop.setXmlOrderKey(key);
            orderEnvelop.setXmlOrderValue(value);
            try {
                JAXBContext jaxbContext = JAXBContext.newInstance(Order.class);
                Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();

                //if the order is valid, we tag it with VALID_ORDER
                orderEnvelop.setValidOrder((Order) jaxbUnmarshaller.unmarshal(new StringReader(value)));
                orderEnvelop.setOrderTag(AppConstants.VALID_ORDER);

                //if the address is missing, we tag with ADDRESS_ERROR
                if(orderEnvelop.getValidOrder().getShipTo().getCity().isEmpty()){
                    log.error("Missing destination City");
                    orderEnvelop.setOrderTag(AppConstants.ADDRESS_ERROR);
                }

            } catch (JAXBException e) {
                //if the Unmarshalled fails, we tag with PARSE_ERROR
                log.error("Failed to Unmarshal the incoming XML");
                orderEnvelop.setOrderTag(AppConstants.PARSE_ERROR);
            }
            //we return a key value pair, and we transformed the xml order to an OrderEnvelop
            return KeyValue.pair(orderEnvelop.getOrderTag(), orderEnvelop);
        });
        //we filter for invalid orders and we send them to the ERROR_TOPIC using the to() method
        //also, we define a Serde configuration here in Produced.with(key,value) to define this custom scenario
        //we define a custom serde because we dont have a channel for the error-topic
        orderEnvelopKStream.filter((k, v) -> !k.equalsIgnoreCase(AppConstants.VALID_ORDER))
                .to(ERROR_TOPIC, Produced.with(AppSerdes.String(), AppSerdes.OrderEnvelop()));

        KStream<String, Order> validOrders = orderEnvelopKStream
                .filter((k, v) -> k.equalsIgnoreCase(AppConstants.VALID_ORDER))
                .map((k, v) -> KeyValue.pair(v.getValidOrder().getOrderId(), v.getValidOrder()));

        validOrders.foreach((k, v) -> log.info(String.format("Valid Order with ID: %s", v.getOrderId())));

        //the valid orders split into two streams, one for india and other for abroad
        //we can do that split using the branch method, it takes comma separated list of predicates.
        //the predicates return a boolean. We need two branches so we pass two predicates, but it can
        //be passed 3 or 5 branches/predicates.
        Predicate<String, Order> isIndiaOrder = (k, v) -> v.getShipTo().getCountry().equalsIgnoreCase("india");
        Predicate<String, Order> isAbroadOrder = (k, v) -> !v.getShipTo().getCountry().equalsIgnoreCase("india");

        return validOrders.branch(isIndiaOrder, isAbroadOrder);

    }

}
