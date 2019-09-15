package org.axesoft.tans.server;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.axesoft.jaxos.algo.StateMachine;
import org.axesoft.tans.protobuff.TansMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class TansService implements StateMachine {
    private static Logger logger = LoggerFactory.getLogger(TansService.class);

    private ConcurrentHashMap<String, TansNumber> numbers = new ConcurrentHashMap<>();

    @Override
    public void consume(long instanceId, ByteString message) {
        TansNumber n1 = fromMessage(message);

        numbers.compute(n1.name(), (k, n0) -> {
            if (n0 == null) {
                return n1;
            }
            else if (n1.version() == n0.version() + 1) {
                return n1;
            }
            else {
                throw new RuntimeException(String.format("Unmatched version n0 = %s, n1 = %s", n0, n1));
            }
        });
    }

    public synchronized ByteString acquire(String name, long v){
        TansNumber n0 = numbers.get(name), n1;
        if(n0 == null){
            n1 = new TansNumber(name, v);
        } else {
            n1 = n0.update(v);
        }

        return toMessage(n1);
    }

    private ByteString toMessage(TansNumber n){
        return TansMessage.ProtoTansNumber.newBuilder()
                .setName(n.name())
                .setValue(n.value())
                .setVersion(n.value())
                .setTimestamp(n.timestamp())
                .build()
                .toByteString();
    }
    private TansNumber fromMessage(ByteString message) {
        try {
            TansMessage.ProtoTansNumber number = TansMessage.ProtoTansNumber.parseFrom(message);
            return new TansNumber(number.getName(), number.getVersion(), number.getTimestamp(), number.getValue());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
