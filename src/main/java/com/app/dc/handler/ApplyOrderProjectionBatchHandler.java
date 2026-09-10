package com.app.dc.handler;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.app.common.utils.Consts;
import com.app.dc.projection.OrderProjectionService;
import com.gw.common.utils.ContentHandler;
import com.gw.common.utils.Message;

@Service("applyOrderProjectionBatch")
public class ApplyOrderProjectionBatchHandler extends ContentHandler {
    @Autowired
    private OrderProjectionService projectionService;

    @Override
    public Map<String, Object> handle(String topic, Message message, String content,
            Map<String, Object> response, boolean fromList) {
        try {
            OrderProjectionService.Watermark watermark = projectionService.applyBatch(content);
            response.put(Consts.Code, Consts.SuccessCode);
            response.put(Consts.Msg, Consts.SuccessMsg);
            response.put("info1", Long.toString(watermark.epoch));
            response.put("info2", Long.toString(watermark.seq));
        } catch (OrderProjectionService.SequenceMismatchException e) {
            logger.warn("order projection gap partition:{}, receiver watermark:{}", e.partitionId, e.current);
            response.put(Consts.Code, Consts.NoKnowCode);
            response.put(Consts.Msg, "PROJECTION_GAP");
            response.put("info1", Long.toString(e.current.epoch));
            response.put("info2", Long.toString(e.current.seq));
            response.put("info3", "GAP");
            response.put("info4", Long.toString(e.current.seq + 1L));
        } catch (Exception e) {
            logger.error("apply order projection batch failed", e);
            response.put(Consts.Code, Consts.NoKnowCode);
            response.put(Consts.Msg, e.getMessage());
        }
        return response;
    }
}
