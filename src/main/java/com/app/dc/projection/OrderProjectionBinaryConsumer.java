package com.app.dc.projection;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.app.common.partition.PartitionAssignment;
import com.app.common.partition.PartitionRouter;
import com.app.dc.projection.OrderProjectionService.Watermark;
import com.app.dc.projection.wire.OrderProjectionFetchRequest;
import com.app.dc.projection.wire.OrderProjectionFetchResponse;
import com.app.dc.projection.wire.OrderProjectionWireBatch;
import com.app.dc.projection.wire.OrderProjectionWireCodec;
import com.app.dc.projection.wire.OrderProjectionWireProtocol;
import com.gateway.connector.tcp.client.GateWayApi;
import com.gateway.connector.tcp.client.IMessage;
import com.gateway.connector.tcp.client.IMessageByte;
import com.gateway.connector.tcp.client.PRspCallBack;
import com.gw.common.utils.GwClientResource;
import com.gw.common.utils.GwClientWrapper;

/**
 * Consumer-owned Projection stream.
 *
 * Realtime committed mutations are pushed by OrderSvr, while any missing range
 * is pulled from the current partition Primary using the durable MySQL watermark.
 * A rotating safety pull also detects a lost final notification when no later
 * event exists to expose the gap.
 */
@Component
@ConditionalOnProperty(name = "projection.binary.enabled", havingValue = "true")
public class OrderProjectionBinaryConsumer implements CommandLineRunner, IMessageByte {
    private static final Logger log = LoggerFactory.getLogger(OrderProjectionBinaryConsumer.class);

    @Autowired
    private GwClientResource gwClientResource;
    @Autowired
    private OrderProjectionService projectionService;

    @Value("${projection.binary.orderServerKey:SERVER.OrderSvr}")
    private String orderServerKey;
    @Value("${projection.binary.fetchMaxRecords:500}")
    private int fetchMaxRecords;
    @Value("${projection.binary.fetchTimeoutMs:5000}")
    private long fetchTimeoutMs;
    @Value("${projection.binary.retryMs:1000}")
    private long retryMs;
    @Value("${projection.binary.subscriptionRefreshMs:5000}")
    private long subscriptionRefreshMs;
    @Value("${projection.binary.safetyPollMillis:1000}")
    private long safetyPollMillis;
    @Value("${projection.binary.safetyPollPartitionsPerRun:16}")
    private int safetyPollPartitionsPerRun;
    @Value("${projection.binary.workerStripes:4}")
    private int workerStripes;
    @Value("${projection.binary.maxBufferedBatchesPerPartition:1024}")
    private int maxBufferedBatchesPerPartition;

    private final Map<String, PartitionState> states = new ConcurrentHashMap<String, PartitionState>();
    private final IMessage<String> ignoredStringListener = new IMessage<String>() {
        @Override
        public void onMessage(String topic, String content) {
            // Projection committed topics are consumed by IMessageByte before this path.
        }
    };
    private volatile IMessageByte previousFilter;
    private volatile GwClientWrapper client;
    private volatile PartitionRouter partitionRouter;
    private volatile ScheduledExecutorService scheduler;
    private volatile ExecutorService[] stripes;
    private volatile boolean running;
    private int safetyCursor;

    @Override
    public void run(String... args) throws Exception {
        if (fetchMaxRecords <= 0 || fetchMaxRecords > 500 || fetchTimeoutMs <= 0L || retryMs <= 0L
                || subscriptionRefreshMs <= 0L || safetyPollMillis <= 0L || safetyPollPartitionsPerRun <= 0
                || workerStripes <= 0 || maxBufferedBatchesPerPartition <= 0) {
            throw new IllegalStateException("invalid projection binary consumer configuration");
        }
        client = gwClientResource.getGwClientWrapper();
        client.init(orderServerKey);
        partitionRouter = new PartitionRouter(orderServerKey);
        stripes = new ExecutorService[workerStripes];
        for (int i = 0; i < stripes.length; i++) {
            final int index = i;
            stripes[i] = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r, "projection-binary-" + index);
                thread.setDaemon(true);
                return thread;
            });
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "projection-binary-control");
            thread.setDaemon(true);
            return thread;
        });

        previousFilter = GateWayApi.Filter;
        GateWayApi.Filter = this;
        running = true;

        refreshSubscription();
        scheduler.scheduleWithFixedDelay(this::safeRefreshSubscription,
                subscriptionRefreshMs, subscriptionRefreshMs, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::safeSafetyPoll,
                0L, safetyPollMillis, TimeUnit.MILLISECONDS);
        log.info("Projection binary consumer started orderServerKey:{}, stripes:{}, fetchMaxRecords:{}, "
                        + "safetyPollMillis:{}, partitionsPerRun:{}",
                orderServerKey, workerStripes, fetchMaxRecords, safetyPollMillis, safetyPollPartitionsPerRun);
    }

    @Override
    public boolean onMessage(String topic, byte[] body) {
        if (!OrderProjectionWireProtocol.isCommittedTopic(topic)) {
            IMessageByte previous = previousFilter;
            return previous == null || previous.onMessage(topic, body);
        }
        try {
            OrderProjectionWireBatch batch = OrderProjectionWireCodec.decodeBatch(body);
            submit(batch.getPartitionId(), () -> onLiveBatch(batch));
        } catch (Exception e) {
            log.error("Invalid OrderSvr projection binary notification topic:{}", topic, e);
        }
        // We own this topic; never continue into the legacy String/JSON path.
        return false;
    }

    private void onLiveBatch(OrderProjectionWireBatch batch) {
        if (!running || batch == null || batch.getEvents() == null || batch.getEvents().isEmpty()) {
            return;
        }
        PartitionState state = state(batch.getPartitionId());
        try {
            ensureWatermark(state);
            if (OrderProjectionSequence.alreadyApplied(state.watermark.epoch, state.watermark.seq, batch)) {
                return;
            }
            if (state.mode != Mode.RUNNING) {
                buffer(state, batch);
                if (state.mode == Mode.GAP_RECOVERING) {
                    requestGap(state);
                }
                return;
            }
            if (!OrderProjectionSequence.continues(state.watermark.epoch, state.watermark.seq, batch)) {
                buffer(state, batch);
                state.mode = Mode.GAP_RECOVERING;
                requestGap(state);
                return;
            }
            state.watermark = projectionService.applyWireBatch(batch);
            drainBuffered(state);
        } catch (Exception e) {
            log.warn("Projection live batch failed partition:{}", batch.getPartitionId(), e);
            buffer(state, batch);
            state.mode = Mode.GAP_RECOVERING;
            requestGap(state);
        }
    }

    private void requestGap(PartitionState state) {
        if (!running || state.fetchInFlight || state.mode == Mode.REBASE_REQUIRED) {
            return;
        }
        try {
            ensureWatermark(state);
            state.fetchInFlight = true;
            OrderProjectionFetchRequest request = new OrderProjectionFetchRequest();
            request.setPartitionId(state.partitionId);
            request.setWatermarkEpoch(state.watermark.epoch);
            request.setWatermarkSeq(state.watermark.seq);
            request.setMaxRecords(fetchMaxRecords);

            PRspCallBack callback = new PRspCallBack();
            callback.rspCallBackByte = bytes -> submit(state.partitionId, () -> onFetchResponse(state, bytes));
            client.requestAsyncToPartition(null, orderServerKey, OrderProjectionWireProtocol.FETCH_METHOD,
                    state.partitionId, OrderProjectionWireCodec.encodeFetchRequest(request), fetchTimeoutMs, callback);
        } catch (Exception e) {
            state.fetchInFlight = false;
            scheduleRetry(state, e);
        }
    }

    private void onFetchResponse(PartitionState state, byte[] bytes) {
        state.fetchInFlight = false;
        if (!running) {
            return;
        }
        if (bytes == null || bytes.length == 0) {
            scheduleRetry(state, new IllegalStateException("empty GAP response"));
            return;
        }
        try {
            OrderProjectionFetchResponse response = OrderProjectionWireCodec.decodeFetchResponse(bytes);
            switch (response.getStatus()) {
            case OK:
                applyFetchResponse(state, response);
                return;
            case BASELINE_MOVED:
                state.mode = Mode.REBASE_REQUIRED;
                state.buffered.clear();
                log.error("PROJECTION_REBASE_REQUIRED partition:{}, watermark:{}, baselineSeq:{}, committedHigh:{}",
                        state.partitionId, state.watermark, response.getBaselineSeq(),
                        response.getCommittedHighWatermark());
                return;
            case NOT_PRIMARY:
            case STALE_EPOCH:
                state.mode = Mode.GAP_RECOVERING;
                scheduleRetry(state, new IllegalStateException(response.getStatus() + ": " + response.getMessage()));
                return;
            default:
                state.mode = Mode.GAP_RECOVERING;
                scheduleRetry(state, new IllegalStateException(response.getStatus() + ": " + response.getMessage()));
            }
        } catch (Exception e) {
            state.mode = Mode.GAP_RECOVERING;
            scheduleRetry(state, e);
        }
    }

    private void applyFetchResponse(PartitionState state, OrderProjectionFetchResponse response) throws Exception {
        OrderProjectionWireBatch batch = response.getBatch();
        if (batch != null && batch.getEvents() != null && !batch.getEvents().isEmpty()) {
            if (!OrderProjectionSequence.continues(state.watermark.epoch, state.watermark.seq, batch)) {
                throw new IllegalStateException("GAP response does not continue durable watermark partition="
                        + state.partitionId + ", watermark=" + state.watermark);
            }
            state.watermark = projectionService.applyWireBatch(batch);
        }

        if (state.watermark.seq < response.getCommittedHighWatermark()) {
            state.mode = Mode.GAP_RECOVERING;
            requestGap(state);
            return;
        }
        state.mode = Mode.RUNNING;
        drainBuffered(state);
    }

    private void drainBuffered(PartitionState state) throws Exception {
        while (state.mode == Mode.RUNNING && !state.buffered.isEmpty()) {
            Map.Entry<Long, OrderProjectionWireBatch> entry = state.buffered.firstEntry();
            OrderProjectionWireBatch batch = entry.getValue();
            if (OrderProjectionSequence.alreadyApplied(state.watermark.epoch, state.watermark.seq, batch)) {
                state.buffered.pollFirstEntry();
                continue;
            }
            if (!OrderProjectionSequence.continues(state.watermark.epoch, state.watermark.seq, batch)) {
                state.mode = Mode.GAP_RECOVERING;
                requestGap(state);
                return;
            }
            state.buffered.pollFirstEntry();
            state.watermark = projectionService.applyWireBatch(batch);
        }
    }

    private void buffer(PartitionState state, OrderProjectionWireBatch batch) {
        if (batch == null || batch.getEvents() == null || batch.getEvents().isEmpty()) {
            return;
        }
        long firstSeq = batch.getEvents().get(0).getJournalSeq();
        state.buffered.put(firstSeq, batch);
        if (state.buffered.size() > maxBufferedBatchesPerPartition) {
            // CQ is authoritative. Dropping the volatile realtime buffer is safe
            // because GAP recovery will continue until OrderSvr's committed high watermark.
            state.buffered.clear();
            log.warn("Projection realtime buffer reset partition:{}; GAP fetch remains authoritative",
                    state.partitionId);
        }
    }

    private PartitionState state(String partitionId) {
        return states.computeIfAbsent(partitionId, PartitionState::new);
    }

    private void ensureWatermark(PartitionState state) throws Exception {
        if (state.watermark == null) {
            state.watermark = projectionService.queryWatermark(state.partitionId);
        }
    }

    /**
     * Rotates across the active partition namespace instead of polling every
     * partition at once. During the async fetch, live batches are buffered on
     * the same stripe so a stale safety response can never race the DB watermark.
     */
    private void safetyPoll() {
        if (!running) {
            return;
        }
        List<PartitionAssignment> assignments = partitionRouter.currentAssignments();
        if (assignments == null || assignments.isEmpty()) {
            return;
        }
        int count = Math.min(safetyPollPartitionsPerRun, assignments.size());
        int start = safetyCursor;
        if (start < 0 || start >= assignments.size()) {
            start = 0;
        }
        for (int i = 0; i < count; i++) {
            PartitionAssignment assignment = assignments.get((start + i) % assignments.size());
            if (assignment == null || !assignment.isRoutable()) {
                continue;
            }
            final String partitionId = assignment.getPartitionId();
            submit(partitionId, () -> {
                PartitionState state = state(partitionId);
                if (state.mode == Mode.REBASE_REQUIRED || state.fetchInFlight) {
                    return;
                }
                try {
                    ensureWatermark(state);
                    state.mode = Mode.GAP_RECOVERING;
                    requestGap(state);
                } catch (Exception e) {
                    state.mode = Mode.GAP_RECOVERING;
                    scheduleRetry(state, e);
                }
            });
        }
        safetyCursor = (start + count) % assignments.size();
    }

    private void safeSafetyPoll() {
        try {
            safetyPoll();
        } catch (Exception e) {
            log.warn("Projection durable safety poll failed", e);
        }
    }

    private void refreshSubscription() {
        client.subscribe(orderServerKey, OrderProjectionWireProtocol.COMMITTED_TOPIC_WILDCARD,
                ignoredStringListener);
    }

    private void safeRefreshSubscription() {
        if (!running) {
            return;
        }
        try {
            refreshSubscription();
        } catch (Exception e) {
            log.warn("Projection binary subscription refresh failed", e);
        }
    }

    private void scheduleRetry(PartitionState state, Exception error) {
        log.warn("Projection GAP retry partition:{}, watermark:{}", state.partitionId, state.watermark, error);
        ScheduledExecutorService current = scheduler;
        if (current != null && running && state.mode != Mode.REBASE_REQUIRED) {
            current.schedule(() -> submit(state.partitionId, () -> requestGap(state)), retryMs, TimeUnit.MILLISECONDS);
        }
    }

    private void submit(String partitionId, Runnable task) {
        ExecutorService[] current = stripes;
        if (!running || current == null || current.length == 0) {
            return;
        }
        int hash = partitionId == null ? 0 : partitionId.hashCode();
        int index = (hash & Integer.MAX_VALUE) % current.length;
        current[index].execute(task);
    }

    @PreDestroy
    public void close() {
        running = false;
        if (GateWayApi.Filter == this) {
            GateWayApi.Filter = previousFilter;
        }
        ScheduledExecutorService currentScheduler = scheduler;
        scheduler = null;
        if (currentScheduler != null) {
            currentScheduler.shutdownNow();
        }
        ExecutorService[] currentStripes = stripes;
        stripes = null;
        if (currentStripes != null) {
            for (ExecutorService stripe : currentStripes) {
                if (stripe != null) {
                    stripe.shutdownNow();
                }
            }
        }
        states.clear();
    }

    private enum Mode {
        RUNNING,
        GAP_RECOVERING,
        REBASE_REQUIRED
    }

    private static final class PartitionState {
        final String partitionId;
        final TreeMap<Long, OrderProjectionWireBatch> buffered = new TreeMap<Long, OrderProjectionWireBatch>();
        Watermark watermark;
        Mode mode = Mode.RUNNING;
        boolean fetchInFlight;

        PartitionState(String partitionId) {
            this.partitionId = partitionId;
        }
    }
}
