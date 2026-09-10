package com.app.dc.handler;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.app.common.utils.Consts;
import com.app.dc.projection.OrderProjectionService;

public class ApplyOrderProjectionBatchHandlerTest {
    @Test
    public void sequenceMismatchMustReturnReceiverWatermarkForJournalReplay() throws Exception {
        ApplyOrderProjectionBatchHandler handler = new ApplyOrderProjectionBatchHandler();
        Field service = ApplyOrderProjectionBatchHandler.class.getDeclaredField("projectionService");
        service.setAccessible(true);
        service.set(handler, new GapProjectionService());

        Map<String, Object> response = handler.handle("applyOrderProjectionBatch", null, "[]",
                new HashMap<String, Object>(), false);

        assertEquals(Consts.NoKnowCode, response.get(Consts.Code));
        assertEquals("PROJECTION_GAP", response.get(Consts.Msg));
        assertEquals("7", response.get("info1"));
        assertEquals("41", response.get("info2"));
        assertEquals("GAP", response.get("info3"));
        assertEquals("42", response.get("info4"));
    }

    private static final class GapProjectionService extends OrderProjectionService {
        @Override
        public Watermark applyBatch(String content) {
            throw new SequenceMismatchException("P027", new Watermark(7L, 41L), 7L, 44L, 7L, 46L);
        }
    }
}
