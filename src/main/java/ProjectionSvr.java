import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.gw.common.utils.ServerGwWrapper;

@SpringBootApplication
@ComponentScan({ "com.gw.common.utils", "com.app.dc", "com.app.common.db" })
@EnableAutoConfiguration
public class ProjectionSvr {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        PropertyConfigurator.configureAndWatch("./config/log4j.ini", 1000);
        Logger logger = LoggerFactory.getLogger(ProjectionSvr.class);
        try {
            ServerGwWrapper.Start(ProjectionSvr.class, "./config/application.properties", args);
        } catch (Exception e) {
            logger.error("ProjectionSvr start error", e);
            throw e;
        }
    }
}
