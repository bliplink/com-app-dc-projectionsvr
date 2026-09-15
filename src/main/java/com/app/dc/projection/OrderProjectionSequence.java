package com.app.dc.projection;

import com.app.dc.projection.wire.OrderProjectionWireBatch;
import com.app.dc.projection.wire.OrderProjectionWireEvent;

/** Pure sequence rules shared by the consumer and unit tests. */
final class OrderProjectionSequence {
    private OrderProjectionSequence() { }

    static boolean continues(long watermarkEpoch, long watermarkSeq, OrderProjectionWireBatch batch) {
        if (batch == null || batch.getEvents() == null || batch.getEvents().isEmpty()) {
            return true;
        }
        OrderProjectionWireEvent first = batch.getEvents().get(0);
        return first.getPreviousEpoch() == watermarkEpoch && first.getPreviousSeq() == watermarkSeq;
    }

    static boolean alreadyApplied(long watermarkEpoch, long watermarkSeq, OrderProjectionWireBatch batch) {
        if (batch == null || batch.getEvents() == null || batch.getEvents().isEmpty()) {
            return true;
        }
        OrderProjectionWireEvent last = batch.getEvents().get(batch.getEvents().size() - 1);
        return last.getEpoch() < watermarkEpoch
                || (last.getEpoch() == watermarkEpoch && last.getJournalSeq() <= watermarkSeq);
    }
}
