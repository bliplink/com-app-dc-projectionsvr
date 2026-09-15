package com.app.dc.projection;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.app.common.db.DBUtils;
import com.app.dc.projection.wire.OrderProjectionWireBatch;
import com.app.dc.projection.wire.OrderProjectionWireEvent;
import com.app.dc.projection.wire.TradeProjectionWireProtocol;

/** Transactional, source-isolated projection of committed TradeSvr STATE_BATCH events. */
@Component
public class TradeProjectionService {
    private static final int MAX_EVENTS = 500;

    public Watermark applyWireBatch(OrderProjectionWireBatch batch) throws Exception {
        if (batch == null || batch.getEvents() == null || batch.getEvents().isEmpty()) {
            throw new IllegalArgumentException("trade projection wire batch is empty");
        }
        if (batch.getEvents().size() > MAX_EVENTS) {
            throw new IllegalArgumentException("trade projection wire batch exceeds " + MAX_EVENTS);
        }
        requireText(batch.getPartitionId(), "partitionId");

        Connection connection = requireConnection();
        try {
            connection.setAutoCommit(false);
            Watermark result = null;
            for (OrderProjectionWireEvent event : batch.getEvents()) {
                if (event == null || !batch.getPartitionId().equals(event.getPartitionId())) {
                    throw new IllegalArgumentException("trade projection wire batch crosses partitions");
                }
                result = applyEvent(event, connection);
            }
            connection.commit();
            return result == null ? Watermark.EMPTY : result;
        } catch (Exception e) {
            try { connection.rollback(); } catch (Exception ignored) { }
            throw e;
        } finally {
            try { connection.setAutoCommit(true); } catch (Exception ignored) { }
            DBUtils.getDatabaseConnection().freeConnection(connection);
        }
    }

    public Watermark queryWatermark(String partitionId) throws Exception {
        requireText(partitionId, "partitionId");
        List<Map<String, Object>> rows = DBUtils.queryListThrowsException(
                "SELECT source_epoch,journal_seq FROM dc_trade_projection_watermark WHERE partition_id=?",
                new Object[] { partitionId });
        return rows.isEmpty() ? Watermark.EMPTY : watermark(rows.get(0));
    }

    private Watermark applyEvent(OrderProjectionWireEvent event, Connection connection) throws Exception {
        validate(event);
        String payload = new String(event.getPayload(), StandardCharsets.UTF_8);
        JSONObject batch = JSON.parseObject(payload);
        if (batch == null || batch.getIntValue("version") != 1) {
            throw new IllegalArgumentException("unsupported TradeSvr state batch version");
        }
        String location = required(batch.getString("location"), "location");
        JSONArray mutations = batch.getJSONArray("mutations");
        if (mutations == null) {
            throw new IllegalArgumentException("TradeSvr state batch mutations are required");
        }

        DBUtils.update("INSERT IGNORE INTO dc_trade_projection_watermark "
                + "(partition_id,source_epoch,journal_seq,update_time) VALUES (?,0,0,NOW(3))",
                new Object[] { event.getPartitionId() }, connection);
        Watermark current = lockWatermark(event.getPartitionId(), connection);
        if (!after(event.getEpoch(), event.getJournalSeq(), current)) {
            return current;
        }
        if (current.epoch != event.getPreviousEpoch() || current.seq != event.getPreviousSeq()) {
            throw new SequenceMismatchException(event.getPartitionId(), current,
                    event.getPreviousEpoch(), event.getPreviousSeq(), event.getEpoch(), event.getJournalSeq());
        }

        int inserted = DBUtils.update("INSERT IGNORE INTO dc_trade_projection_event "
                + "(event_id,partition_id,source_epoch,journal_seq,event_type,source_node,event_time,payload,create_time) "
                + "VALUES (?,?,?,?,?,?,?,?,NOW(3))",
                new Object[] { event.getEventId(), event.getPartitionId(), event.getEpoch(), event.getJournalSeq(),
                        event.getEventType(), event.getSourceNode(), event.getEventTimestamp(), payload }, connection);
        if (inserted != 1) {
            throw new IllegalStateException("trade projection event exists ahead of watermark: " + event.getEventId());
        }

        String requestId = batch.getString("requestId");
        String sourceType = batch.getString("sourceType");
        for (int i = 0; i < mutations.size(); i++) {
            JSONObject mutation = mutations.getJSONObject(i);
            validateMutation(mutation, event, i);
            DBUtils.update("INSERT INTO dc_trade_projection_mutation "
                    + "(event_id,mutation_index,partition_id,source_epoch,journal_seq,location,source_type,request_id,"
                    + "entity_type,operation_type,entity_key,payload,create_time) "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NOW(3))",
                    new Object[] { event.getEventId(), Integer.valueOf(i), event.getPartitionId(), event.getEpoch(),
                            event.getJournalSeq(), location, sourceType, requestId, mutation.getString("entityType"),
                            mutation.getString("operation"), mutation.getString("entityKey"),
                            mutation.getString("payload") }, connection);
        }

        DBUtils.update("UPDATE dc_trade_projection_watermark SET source_epoch=?,journal_seq=?,event_id=?,update_time=NOW(3) "
                + "WHERE partition_id=?",
                new Object[] { event.getEpoch(), event.getJournalSeq(), event.getEventId(), event.getPartitionId() },
                connection);
        return new Watermark(event.getEpoch(), event.getJournalSeq());
    }

    private void validate(OrderProjectionWireEvent event) {
        if (event.getVersion() != TradeProjectionWireProtocol.VERSION) {
            throw new IllegalArgumentException("unsupported trade projection wire event version");
        }
        requireText(event.getEventId(), "eventId");
        requireText(event.getPartitionId(), "partitionId");
        requireText(event.getEventType(), "eventType");
        if (!"STATE_BATCH".equals(event.getEventType())) {
            throw new IllegalArgumentException("unexpected TradeSvr projection event type: " + event.getEventType());
        }
        if (event.getEpoch() <= 0L || event.getJournalSeq() <= 0L || event.getPayload() == null) {
            throw new IllegalArgumentException("invalid trade projection wire event");
        }
    }

    private void validateMutation(JSONObject mutation, OrderProjectionWireEvent event, int index) {
        if (mutation == null || mutation.getIntValue("version") != 1) {
            throw new IllegalArgumentException("invalid TradeSvr mutation event=" + event.getEventId()
                    + ", index=" + index);
        }
        requireText(mutation.getString("entityType"), "entityType");
        String operation = required(mutation.getString("operation"), "operation");
        if (!"UPSERT".equals(operation) && !"REMOVE".equals(operation)) {
            throw new IllegalArgumentException("unsupported TradeSvr mutation operation: " + operation);
        }
    }

    private Watermark lockWatermark(String partitionId, Connection connection) throws Exception {
        List<Map<String, Object>> rows = DBUtils.queryListThrowsException(
                "SELECT source_epoch,journal_seq FROM dc_trade_projection_watermark WHERE partition_id=? FOR UPDATE",
                new Object[] { partitionId }, connection);
        if (rows.isEmpty()) throw new IllegalStateException("trade projection watermark row missing: " + partitionId);
        return watermark(rows.get(0));
    }

    private Watermark watermark(Map<String, Object> row) {
        return new Watermark(number(row, "source_epoch"), number(row, "journal_seq"));
    }

    private long number(Map<String, Object> row, String name) {
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (name.equalsIgnoreCase(entry.getKey())) {
                Object value = entry.getValue();
                return value == null ? 0L : ((Number) value).longValue();
            }
        }
        return 0L;
    }

    private static boolean after(long epoch, long seq, Watermark current) {
        return epoch > current.epoch || (epoch == current.epoch && seq > current.seq);
    }

    private Connection requireConnection() {
        Connection connection = DBUtils.getDatabaseConnection().getConnection();
        if (connection == null) throw new IllegalStateException("MySQL connection unavailable");
        return connection;
    }

    private static String required(String value, String name) {
        requireText(value, name);
        return value.trim();
    }

    private static void requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) throw new IllegalArgumentException(name + " is required");
    }

    public static final class Watermark {
        public static final Watermark EMPTY = new Watermark(0L, 0L);
        public final long epoch;
        public final long seq;
        public Watermark(long epoch, long seq) { this.epoch = epoch; this.seq = seq; }
        @Override public String toString() { return epoch + ":" + seq; }
    }

    public static final class SequenceMismatchException extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        public final String partitionId;
        public final Watermark current;
        public SequenceMismatchException(String partitionId, Watermark current, long previousEpoch,
                long previousSeq, long incomingEpoch, long incomingSeq) {
            super("trade projection sequence mismatch partition=" + partitionId + ", current=" + current
                    + ", expected=" + previousEpoch + ":" + previousSeq
                    + ", incoming=" + incomingEpoch + ":" + incomingSeq);
            this.partitionId = partitionId;
            this.current = current;
        }
    }
}
