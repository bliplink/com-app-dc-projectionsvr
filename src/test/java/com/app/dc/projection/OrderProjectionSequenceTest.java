package com.app.dc.projection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import com.app.dc.projection.wire.OrderProjectionWireBatch;
import com.app.dc.projection.wire.OrderProjectionWireEvent;

public class OrderProjectionSequenceTest {

    @Test
    public void sameEpochBatchContinuesDurableWatermark() {
        OrderProjectionWireBatch batch = batch(event(18L, 1005L, 18L, 1002L));
        assertTrue(OrderProjectionSequence.continues(18L, 1002L, batch));
        assertFalse(OrderProjectionSequence.alreadyApplied(18L, 1002L, batch));
    }

    @Test
    public void missingMutationIsDetectedAsGap() {
        OrderProjectionWireBatch batch = batch(event(18L, 1010L, 18L, 1005L));
        assertFalse(OrderProjectionSequence.continues(18L, 1002L, batch));
    }

    @Test
    public void firstMutationOfNewEpochCanContinueOldEpochWatermark() {
        OrderProjectionWireBatch batch = batch(event(19L, 1100L, 18L, 1095L));
        assertTrue(OrderProjectionSequence.continues(18L, 1095L, batch));
        assertFalse(OrderProjectionSequence.alreadyApplied(18L, 1095L, batch));
    }

    @Test
    public void duplicateBatchIsAlreadyApplied() {
        OrderProjectionWireBatch batch = batch(
                event(18L, 1005L, 18L, 1002L),
                event(18L, 1010L, 18L, 1005L));
        assertTrue(OrderProjectionSequence.alreadyApplied(18L, 1010L, batch));
    }

    private static OrderProjectionWireBatch batch(OrderProjectionWireEvent... events) {
        OrderProjectionWireBatch batch = new OrderProjectionWireBatch();
        batch.setPartitionId("P037");
        batch.setEpoch(events.length == 0 ? 0L : events[events.length - 1].getEpoch());
        batch.setCommittedHighWatermark(events.length == 0 ? 0L : events[events.length - 1].getJournalSeq());
        batch.setEvents(Arrays.asList(events));
        return batch;
    }

    private static OrderProjectionWireEvent event(long epoch, long seq, long previousEpoch, long previousSeq) {
        OrderProjectionWireEvent event = new OrderProjectionWireEvent();
        event.setEventId("P037:" + epoch + ":" + seq);
        event.setPartitionId("P037");
        event.setEpoch(epoch);
        event.setJournalSeq(seq);
        event.setEventType("STATE_UPSERT");
        event.setPreviousEpoch(previousEpoch);
        event.setPreviousSeq(previousSeq);
        event.setPayload(new byte[] { 1 });
        return event;
    }
}
