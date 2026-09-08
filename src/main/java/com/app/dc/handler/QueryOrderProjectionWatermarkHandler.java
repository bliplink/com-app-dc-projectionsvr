package com.app.dc.handler;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.app.common.utils.Consts;
import com.app.dc.projection.OrderProjectionService;
import com.gw.common.utils.ContentHandler;
import com.gw.common.utils.Message;

@Service("queryOrderProjectionWatermark")
public class QueryOrderProjectionWatermarkHandler extends ContentHandler {
    @Autowired
    private OrderProjectionService projectionService;

    @Override
    public Map<String, Object> handle(String topic, Message message, String content,
            Map<String, Object> response, boolean fromList) {
        try {
            JSONObject request = JSON.parseObject(content);
            String partitionId = request == null ? null : request.getString("partitionId");
            OrderProjectionService.Watermark watermark = projectionService.queryWatermark(partitionId);
            response.put(Consts.Code, Consts.SuccessCode);
            response.put(Consts.Msg, Consts.SuccessMsg);
            response.put("info1", Long.toString(watermark.epoch));
            response.put("info2", Long.toString(watermark.seq));
        } catch (Exception e) {
            logger.error("query projection watermark failed", e);
            response.put(Consts.Code, Consts.NoKnowCode);
            response.put(Consts.Msg, e.getMessage());
        }
        return response;
    }
}
