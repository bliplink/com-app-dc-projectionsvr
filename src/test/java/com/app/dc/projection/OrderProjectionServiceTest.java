package com.app.dc.projection;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Method;

import org.junit.Test;

import com.alibaba.fastjson2.JSONObject;

public class OrderProjectionServiceTest {
    @Test
    public void shouldProjectCurrentUnOpenQuantityAsLeavesQuantity() throws Exception {
        JSONObject order = new JSONObject();
        order.put("unOpenQty", "0.001");
        order.put("unCumQty", "9.999");

        assertEquals("0.001", orderParams(order)[21]);
    }

    @Test
    public void shouldKeepLegacyUnCumQuantityCompatibility() throws Exception {
        JSONObject order = new JSONObject();
        order.put("unCumQty", "0.002");

        assertEquals("0.002", orderParams(order)[21]);
    }

    private Object[] orderParams(JSONObject order) throws Exception {
        Method method = OrderProjectionService.class.getDeclaredMethod("orderParams", JSONObject.class);
        method.setAccessible(true);
        return (Object[]) method.invoke(new OrderProjectionService(), order);
    }
}
