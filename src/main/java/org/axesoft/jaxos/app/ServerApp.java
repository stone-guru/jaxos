package org.axesoft.jaxos.app;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.axesoft.jaxos.JaxosConfig;
import org.axesoft.jaxos.httpserver.HttpApiServer;
import org.axesoft.jaxos.netty.NettyJaxosNode;
import org.slf4j.LoggerFactory;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class ServerApp {
    public static void main(String[] args) throws Exception {
        JaxosConfig config = new ArgumentParser().parse(args);
        initLogback(config.serverId());

        NettyJaxosNode server = new NettyJaxosNode(config);
        new Thread(null, () -> server.startup(), "JaxosNodeThread").start();

        HttpApiServer httpServer = new HttpApiServer(server.instance(), config.self().address(), config.self().httpPort());
        httpServer.start();
    }

    public static void initLogback(int id) throws JoranException {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator jc = new JoranConfigurator();
        jc.setContext(context);
        context.reset(); // override default configuration
        // inject the name of the current application as "application-name"
        // property of the LoggerContext
        context.putProperty("node-id", Integer.toString(id));

        jc.doConfigure(ClassLoader.getSystemResource("jaxos-logback.xml"));
    }
}
