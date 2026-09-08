package com.app.dc.handler;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson2.JSON;
import com.app.common.utils.Consts;
import com.app.dc.projection.OrderProjectionService;
import com.gw.common.utils.ContentHandler;
import com.gw.common.utils.Message;

@Service("queryProjectedOrderHistory")
public class QueryProjectedOrderHistoryHandler extends ContentHandler {
    @Autowired
    private OrderProjectionService projectionService;

    @Override
    public Map<String, Object> handle(String topic, Message message, String content,
            Map<String, Object> response, boolean fromList) {
        try {
            OrderProjectionService.HistoryResult result = projectionService.queryOrderHistory(content);
            response.put(Consts.Code, Consts.SuccessCode);
            response.put(Consts.Msg, Consts.SuccessMsg);
            response.put("info1", JSON.toJSONString(result.reports));
            response.put("info2", Integer.toString(result.reports.size()));
        } catch (Exception e) {
            logger.error("query projected order history failed", e);
            response.put(Consts.Code, Consts.NoKnowCode);
            response.put(Consts.Msg, e.getMessage());
        }
        return response;
    }
}
