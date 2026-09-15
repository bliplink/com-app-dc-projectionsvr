package com.app.dc.projection;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.app.common.partition.PartitionConfig;
import com.gw.common.utils.ConfigUtils;

/** Validates prerequisites before the binary consumer runner starts. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@ConditionalOnProperty(name = "projection.binary.enabled", havingValue = "true")
public class OrderProjectionBinaryConfigurationValidator implements CommandLineRunner {
    @Value("${projection.binary.orderServerKey:SERVER.OrderSvr}")
    private String orderServerKey;

    @Override
    public void run(String... args) {
        if (!PartitionConfig.isServiceEnabled(orderServerKey)) {
            throw new IllegalStateException("projection.binary requires partition clustering for " + orderServerKey);
        }
        String loadBalance = ConfigUtils.getLBType(orderServerKey);
        if (!"Partition".equalsIgnoreCase(loadBalance)) {
            throw new IllegalStateException("projection.binary requires LBConfig for " + orderServerKey
                    + " to be Partition; actual=" + loadBalance);
        }
        if (ConfigUtils.getProtoVersion() != 2) {
            throw new IllegalStateException("projection.binary requires gateway ProtoVersion=2");
        }
    }
}
