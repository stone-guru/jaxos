package org.axesoft.tans.statemachine;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.axesoft.jaxos.algo.StateMachine;
import org.axesoft.tans.protobuff.TansMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class TansRepository implements StateMachine {
    private static Logger logger = LoggerFactory.getLogger(TansRepository.class);

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

    public synchronized TansNumber acquire(String name, long v){
        TansNumber n = numbers.get(name);
        if(n == null){
            return new TansNumber(name, v);
        } else {
            return n.update(v);
        }
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
