package com.app.dc.projection;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.app.common.db.DBUtils;
import com.app.dc.fix.message.ExecutionReport;

/** Transactional, idempotent MySQL projection of committed OrderSvr events. */
@Component
public class OrderProjectionService {
    private static final int EVENT_VERSION = 1;

    private static final String ORDER_SQL = "REPLACE INTO dc_orders "
            + "(algo_name,order_id,user_id,account_id,currency,market_indicator,security_id,symbol,clord_id,ref_order_id,side,ord_type,"
            + "timeinforce,oc_type,position_side,reduce_only,price,order_qty,ord_status,ord_Rej_Reason,reject_Text,leaves_qty,cum_qty,"
            + "take_profit_price,stop_loss_price,trigger_type,trigger_price,trigger_condition,create_time,update_time,close_by,location,transact_time,"
            + "info1,info2,info3,info4,info5,maker) VALUES "
            + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String EXEC_SQL = "REPLACE INTO dc_orders_execorders "
            + "(exec_id,order_id,user_id,account_id,currency,security_id,symbol,oc_type,side,exec_type,last_px,last_qty,fee,realized_Pnl,Maker,"
            + "create_time,update_time,close_by,location,transact_time,info1,info2,info3,info4,info5,market_indicator) "
            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Value("${projection.saveDemo:false}")
    private boolean saveDemo;

    public Watermark applyBatch(String content) throws Exception {
        JSONArray events = JSON.parseArray(content);
        if (events == null || events.isEmpty()) {
            throw new IllegalArgumentException("projection event batch is empty");
        }
        if (events.size() > 500) {
            throw new IllegalArgumentException("projection event batch exceeds 500");
        }

        Connection connection = requireConnection();
        try {
            connection.setAutoCommit(false);
            Watermark result = null;
            for (int i = 0; i < events.size(); i++) {
                result = applyEvent(events.getJSONObject(i), connection);
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
                "SELECT source_epoch,journal_seq FROM dc_order_projection_watermark WHERE partition_id=?",
                new Object[] { partitionId });
        return rows.isEmpty() ? Watermark.EMPTY : watermark(rows.get(0));
    }

    /** Reads the durable MySQL order history without making MySQL part of matching or recovery. */
    public HistoryResult queryOrderHistory(String content) throws Exception {
        JSONObject request = JSON.parseObject(content);
        QueryScope scope = QueryScope.from(request);
        StringBuilder sql = new StringBuilder("SELECT * FROM dc_orders WHERE location=? AND user_id=?");
        List<Object> args = new ArrayList<Object>();
        args.add(scope.location);
        args.add(scope.userId);
        appendScope(sql, args, scope);
        sql.append(" ORDER BY create_time DESC LIMIT ?");
        args.add(Integer.valueOf(scope.limit));
        List<Map<String, Object>> rows = DBUtils.queryListThrowsException(sql.toString(), args.toArray());
        List<ExecutionReport> reports = new ArrayList<ExecutionReport>(rows.size());
        for (Map<String, Object> row : rows) {
            reports.add(orderReport(row));
        }
        return new HistoryResult(reports);
    }

    /** Reads the durable MySQL execution history owned by ProjectionSvr. */
    public HistoryResult queryExecutionHistory(String content) throws Exception {
        JSONObject request = JSON.parseObject(content);
        QueryScope scope = QueryScope.from(request);
        StringBuilder sql = new StringBuilder("SELECT * FROM dc_orders_execorders WHERE location=? AND user_id=?");
        List<Object> args = new ArrayList<Object>();
        args.add(scope.location);
        args.add(scope.userId);
        appendScope(sql, args, scope);
        sql.append(" ORDER BY create_time DESC LIMIT ?");
        args.add(Integer.valueOf(scope.limit));
        List<Map<String, Object>> rows = DBUtils.queryListThrowsException(sql.toString(), args.toArray());
        List<ExecutionReport> reports = new ArrayList<ExecutionReport>(rows.size());
        for (Map<String, Object> row : rows) {
            reports.add(executionReport(row));
        }
        return new HistoryResult(reports);
    }

    private void appendScope(StringBuilder sql, List<Object> args, QueryScope scope) {
        if (scope.securityId != null && !"*".equals(scope.securityId)) {
            sql.append(" AND security_id=?");
            args.add(scope.securityId);
        }
        if (scope.orderId != null) {
            sql.append(" AND order_id=?");
            args.add(scope.orderId);
        }
    }

    private ExecutionReport orderReport(Map<String, Object> row) {
        ExecutionReport report = baseReport(row);
        report.setAlgoName(column(row, "algo_name"));
        report.setClOrdID(column(row, "clord_id"));
        report.setRefOrderID(column(row, "ref_order_id"));
        report.setOrdStatus(column(row, "ord_status"));
        report.setOrdType(column(row, "ord_type"));
        report.setTimeInForce(column(row, "timeinforce"));
        report.setOrderQty(column(row, "order_qty"));
        report.setLeavesQty(column(row, "leaves_qty"));
        report.setCumQty(column(row, "cum_qty"));
        report.setPrice(column(row, "price"));
        report.setRejectText(column(row, "reject_Text"));
        report.setOrdRejReason(column(row, "ord_Rej_Reason"));
        report.setPositionSide(column(row, "position_side"));
        report.setReduceOnly(booleanText(row, "reduce_only"));
        report.setTakeProfitPrice(column(row, "take_profit_price"));
        report.setStopLossPrice(column(row, "stop_loss_price"));
        report.setTriggerType(column(row, "trigger_type"));
        report.setTriggerPrice(column(row, "trigger_price"));
        report.setTrigger(column(row, "trigger_condition"));
        return report;
    }

    private ExecutionReport executionReport(Map<String, Object> row) {
        ExecutionReport report = baseReport(row);
        report.setExecID(column(row, "exec_id"));
        report.setExecType(column(row, "exec_type"));
        report.setLastPx(column(row, "last_px"));
        report.setLastQty(column(row, "last_qty"));
        report.setFee(column(row, "fee"));
        report.setRealizedPnl(column(row, "realized_Pnl"));
        return report;
    }

    private ExecutionReport baseReport(Map<String, Object> row) {
        ExecutionReport report = new ExecutionReport();
        report.setOrderID(column(row, "order_id"));
        report.setUserID(column(row, "user_id"));
        report.setAccountID(column(row, "account_id"));
        report.setSecurityID(column(row, "security_id"));
        report.setSymbol(column(row, "symbol"));
        report.setOCType(column(row, "oc_type"));
        report.setSide(column(row, "side"));
        report.setMaker(booleanText(row, "Maker"));
        report.setTransactTime(firstColumn(row, "transact_time", "create_time"));
        report.setUpdateTime(column(row, "update_time"));
        report.setCloseBy(column(row, "close_by"));
        report.setLocation(column(row, "location"));
        report.setMarketIndicator(column(row, "market_indicator"));
        report.setInfo1(column(row, "info1"));
        report.setInfo2(column(row, "info2"));
        report.setInfo3(column(row, "info3"));
        report.setInfo4(column(row, "info4"));
        report.setInfo5(column(row, "info5"));
        return report;
    }

    private String booleanText(Map<String, Object> row, String name) {
        String value = column(row, name);
        return "1".equals(value) || "true".equalsIgnoreCase(value) ? "true" : "false";
    }

    private String firstColumn(Map<String, Object> row, String... names) {
        for (String name : names) {
            String value = column(row, name);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return null;
    }

    private String column(Map<String, Object> row, String name) {
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (name.equalsIgnoreCase(entry.getKey())) {
                return entry.getValue() == null ? null : String.valueOf(entry.getValue());
            }
        }
        return null;
    }

    private Watermark applyEvent(JSONObject event, Connection connection) throws Exception {
        validateEnvelope(event);
        String eventId = event.getString("eventId");
        String partitionId = event.getString("partitionId");
        long epoch = event.getLongValue("epoch");
        long seq = event.getLongValue("journalSeq");
        long previousEpoch = event.getLongValue("previousEpoch");
        long previousSeq = event.getLongValue("previousSeq");

        DBUtils.update("INSERT IGNORE INTO dc_order_projection_watermark "
                + "(partition_id,source_epoch,journal_seq,update_time) VALUES (?,0,0,NOW(3))",
                new Object[] { partitionId }, connection);
        Watermark current = lockWatermark(partitionId, connection);
        if (!after(epoch, seq, current)) {
            return current;
        }
        if (current.epoch != previousEpoch || current.seq != previousSeq) {
            throw new IllegalStateException("projection sequence mismatch partition=" + partitionId
                    + ", current=" + current + ", expected=" + previousEpoch + ":" + previousSeq
                    + ", incoming=" + epoch + ":" + seq);
        }

        int inserted = DBUtils.update("INSERT IGNORE INTO dc_order_projection_event "
                + "(event_id,partition_id,source_epoch,journal_seq,event_type,source_node,event_time,payload,create_time) "
                + "VALUES (?,?,?,?,?,?,?,?,NOW(3))",
                new Object[] { eventId, partitionId, epoch, seq, event.getString("eventType"),
                        event.getString("sourceNode"), event.getLongValue("eventTimestamp"),
                        event.getString("payload") }, connection);
        if (inserted != 1) {
            throw new IllegalStateException("projection event exists ahead of watermark: " + eventId);
        }

        JSONObject mutation = JSON.parseObject(event.getString("payload"));
        JSONObject order = mutation == null ? null : mutation.getJSONObject("order");
        if (order == null) {
            throw new IllegalArgumentException("projection mutation has no final order image: " + eventId);
        }
        if (saveDemo || !isDemo(order)) {
            DBUtils.update(ORDER_SQL, orderParams(order), connection);
            JSONObject execution = mutation.getJSONObject("execution");
            if (execution != null) {
                DBUtils.update(EXEC_SQL, executionParams(execution), connection);
            }
        }

        DBUtils.update("UPDATE dc_order_projection_watermark SET source_epoch=?,journal_seq=?,event_id=?,update_time=NOW(3) "
                + "WHERE partition_id=?",
                new Object[] { epoch, seq, eventId, partitionId }, connection);
        return new Watermark(epoch, seq);
    }

    private Watermark lockWatermark(String partitionId, Connection connection) throws Exception {
        List<Map<String, Object>> rows = DBUtils.queryListThrowsException(
                "SELECT source_epoch,journal_seq FROM dc_order_projection_watermark WHERE partition_id=? FOR UPDATE",
                new Object[] { partitionId }, connection);
        if (rows.isEmpty()) {
            throw new IllegalStateException("projection watermark row missing: " + partitionId);
        }
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

    private boolean after(long epoch, long seq, Watermark current) {
        return epoch > current.epoch || (epoch == current.epoch && seq > current.seq);
    }

    private void validateEnvelope(JSONObject event) {
        if (event == null || event.getIntValue("version") != EVENT_VERSION) {
            throw new IllegalArgumentException("unsupported projection event version");
        }
        requireText(event.getString("eventId"), "eventId");
        requireText(event.getString("partitionId"), "partitionId");
        requireText(event.getString("eventType"), "eventType");
        requireText(event.getString("payload"), "payload");
        if (event.getLongValue("epoch") <= 0L || event.getLongValue("journalSeq") <= 0L) {
            throw new IllegalArgumentException("projection epoch/journalSeq must be positive");
        }
    }

    private Object[] orderParams(JSONObject value) {
        return new Object[] { text(value, "algoName"), text(value, "orderId"), text(value, "userId"),
                text(value, "accountId"), text(value, "currency"), text(value, "marketIndicator"),
                text(value, "securityId"), text(value, "symbol"), text(value, "clOrderId"),
                text(value, "refOrderId"), text(value, "side"), text(value, "orderType"),
                text(value, "timeInForce"), text(value, "OCType", "oCType"), text(value, "positionSide"),
                bool(value, "reduceOnly") ? 1 : 0, value.get("price"), value.get("qty"),
                text(value, "orderStatus"), text(value, "rejCode"), text(value, "rejReason"),
                raw(value, "unOpenQty", "unCumQty"), value.get("cumQty"), value.get("takeProfitPrice"),
                value.get("stopLossPrice"), text(value, "triggerType"), value.get("triggerPrice"),
                text(value, "trigger"), text(value, "createtime"), text(value, "updateTime"),
                text(value, "closeBy"), text(value, "location"), text(value, "transactTime"),
                text(value, "info1"), text(value, "info2"), text(value, "info3"), text(value, "info4"),
                text(value, "info5"), bool(value, "maker") ? 1 : 0 };
    }

    private Object[] executionParams(JSONObject value) {
        return new Object[] { text(value, "execId"), text(value, "orderId"), text(value, "userId"),
                text(value, "accountId"), text(value, "currency"), text(value, "securityId"),
                text(value, "symbol"), text(value, "OCType", "oCType"), text(value, "side"),
                text(value, "execType"), value.get("price"), value.get("qty"), text(value, "fee"),
                text(value, "realizedPnl"), bool(value, "maker") ? 1 : 0, text(value, "createtime"),
                text(value, "updateTime"), text(value, "closeBy"), text(value, "location"),
                text(value, "transactTime"), text(value, "info1"), text(value, "info2"),
                text(value, "info3"), text(value, "info4"), text(value, "info5"),
                text(value, "marketIndicator") };
    }

    private boolean isDemo(JSONObject value) {
        String demo = text(value, "demo");
        return "true".equalsIgnoreCase(demo) || "1".equals(demo);
    }

    private boolean bool(JSONObject value, String key) {
        Object raw = value.get(key);
        return raw instanceof Boolean ? ((Boolean) raw).booleanValue()
                : raw != null && ("true".equalsIgnoreCase(String.valueOf(raw)) || "1".equals(String.valueOf(raw)));
    }

    private String text(JSONObject value, String... keys) {
        for (String key : keys) {
            Object raw = value.get(key);
            if (raw != null) {
                return String.valueOf(raw);
            }
        }
        return null;
    }

    private Object raw(JSONObject value, String... keys) {
        for (String key : keys) {
            Object raw = value.get(key);
            if (raw != null) {
                return raw;
            }
        }
        return null;
    }

    private void requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " is required");
        }
    }

    private Connection requireConnection() {
        Connection connection = DBUtils.getDatabaseConnection().getConnection();
        if (connection == null) {
            throw new IllegalStateException("MySQL connection unavailable");
        }
        return connection;
    }

    public static final class Watermark {
        public static final Watermark EMPTY = new Watermark(0L, 0L);
        public final long epoch;
        public final long seq;

        public Watermark(long epoch, long seq) {
            this.epoch = epoch;
            this.seq = seq;
        }

        @Override public String toString() { return epoch + ":" + seq; }
    }

    public static final class HistoryResult {
        public final List<ExecutionReport> reports;

        HistoryResult(List<ExecutionReport> reports) {
            this.reports = reports;
        }
    }

    private static final class QueryScope {
        final String location;
        final String userId;
        final String securityId;
        final String orderId;
        final int limit;

        private QueryScope(String location, String userId, String securityId, String orderId, int limit) {
            this.location = location;
            this.userId = userId;
            this.securityId = securityId;
            this.orderId = orderId;
            this.limit = limit;
        }

        static QueryScope from(JSONObject request) {
            if (request == null) {
                throw new IllegalArgumentException("history query is required");
            }
            String location = required(request.getString("location"), "location");
            String userId = required(request.getString("userId"), "userId");
            int requested = request.getIntValue("maxOrderCount");
            int limit = requested <= 0 ? 100 : Math.min(requested, 1000);
            return new QueryScope(location, userId, trim(request.getString("securityId")),
                    trim(request.getString("orderId")), limit);
        }

        private static String required(String value, String name) {
            String clean = trim(value);
            if (clean == null) {
                throw new IllegalArgumentException(name + " is required");
            }
            return clean;
        }

        private static String trim(String value) {
            return value == null || value.trim().isEmpty() ? null : value.trim();
        }
    }
}
