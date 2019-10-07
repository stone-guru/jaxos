package org.axesoft.jaxos.logger;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.axesoft.jaxos.algo.AcceptorLogger;
import org.axesoft.jaxos.algo.Event;
import org.axesoft.jaxos.algo.InstanceValue;
import org.axesoft.jaxos.algo.CheckPoint;
import org.axesoft.jaxos.network.protobuff.PaxosMessage;
import org.axesoft.jaxos.network.protobuff.ProtoMessageCoder;
import org.iq80.leveldb.*;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class LevelDbAcceptorLogger implements AcceptorLogger {
    private static final Logger logger = LoggerFactory.getLogger(LevelDbAcceptorLogger.class);

    private static final byte CATEGORY_SQUAD_LAST = 1;
    private static final byte CATEGORY_PROMISE = 2;
    private static final byte CATEGORY_SQUAD_CHECKPOINT = 3;

    private DB db;
    private String path;
    private ProtoMessageCoder messageCoder;

    public LevelDbAcceptorLogger(String path) {
        this.path = path;

        tryCreateDir(path);

        Options options = new Options().createIfMissing(true);
        options.cacheSize();
        try {
            db = Iq80DBFactory.factory.open(new File(path), options);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.messageCoder = new ProtoMessageCoder();
    }

    private void tryCreateDir(String path) {
        File f = new File(path);
        if (f.exists()) {
            if (!f.isDirectory()) {
                throw new RuntimeException(path + " is not a directory");
            }
        }
        else {
            if (!f.mkdir()) {
                throw new RuntimeException("can not mkdir at " + path);
            }
        }
    }

    @Override
    public void savePromise(int squadId, long instanceId, int proposal, Event.BallotValue value) {
        InstanceValue instanceValue = new InstanceValue();
        instanceValue.squadId = squadId;
        instanceValue.instanceId = instanceId;
        instanceValue.proposal = proposal;
        instanceValue.value = value;

        byte[] data = toByteArray(instanceValue);
        WriteOptions writeOptions = new WriteOptions().sync(true);

        WriteBatch writeBatch = db.createWriteBatch();
        byte[] promiseKey = keyOfInstanceId(squadId, instanceId);
        writeBatch.put(promiseKey, data);
        writeBatch.put(keyOfSquadLast(squadId), promiseKey);
        db.write(writeBatch, writeOptions);
    }

    @Override
    public InstanceValue loadLastPromise(int squadId) {
        byte[] last = keyOfSquadLast(squadId);
        byte[] idx = db.get(last);
        if (idx == null) {
            return null;
        }

        byte[] bx = db.get(idx);
        if (bx == null) {
            return null;
        }

        return toEntity(bx);
    }

    @Override
    public InstanceValue loadPromise(int squadId, long instanceId) {
        byte[] key = keyOfInstanceId(squadId, instanceId);
        byte[] bx = db.get(key);

        if (bx != null) {
            return toEntity(bx);
        }
        return null;
    }

    @Override
    public void saveCheckPoint(CheckPoint checkPoint) {
        byte[] key = keyOfCheckPoint(checkPoint.squadId());
        byte[] data = toByteArray(checkPoint);

        WriteOptions writeOptions = new WriteOptions().sync(true);
        db.put(key, data, writeOptions);
    }

    @Override
    public CheckPoint loadLastCheckPoint(int squadId) {
        byte[] key = keyOfCheckPoint(squadId);
        byte[] data = db.get(key);

        if (data == null) {
            return null;
        }
        return toCheckPoint(data);
    }

    @Override
    public void close() {
        try {
            this.db.close();
        }
        catch (IOException e) {
            logger.error("Error when close db in " + this.path);
        }
    }

    public byte[] toByteArray(CheckPoint checkPoint) {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(bs);
        try {
            os.writeInt(checkPoint.squadId());
            os.writeLong(checkPoint.instanceId());
            os.writeLong(checkPoint.timestamp());
            os.writeInt(checkPoint.content().size());
            os.write(checkPoint.content().toByteArray());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return bs.toByteArray();
    }

    public CheckPoint toCheckPoint(byte[] bytes) {
        DataInputStream is = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            int squadId = is.readInt();
            long instanceId = is.readLong();
            long timestamp = is.readLong();
            int sz = is.readInt();
            byte[] content = is.readNBytes(sz);
            return new CheckPoint(squadId, instanceId, timestamp, ByteString.copyFrom(content));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public InstanceValue toEntity(byte[] bytes) {
        PaxosMessage.InstanceValue i = null;
        try {
            i = PaxosMessage.InstanceValue.parseFrom(bytes);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        Event.BallotValue v = messageCoder.decodeValue(i.getValue());
        return new InstanceValue(i.getSquadId(), i.getInstanceId(), i.getProposal(), v);
    }


    public byte[] toByteArray(InstanceValue v) {
        return PaxosMessage.InstanceValue.newBuilder()
                .setSquadId(v.squadId())
                .setInstanceId(v.instanceId())
                .setProposal(v.proposal())
                .setValue(messageCoder.encodeValue(v.value()))
                .build()
                .toByteArray();
    }

    private byte[] keyOfInstanceId(int squadId, long i) {
        byte[] key = new byte[13];
        key[0] = CATEGORY_PROMISE;
        System.arraycopy(Ints.toByteArray(squadId), 0, key, 1, 4);
        System.arraycopy(Longs.toByteArray(i), 0, key, 5, 8);
        return key;
    }

    private byte[] keyOfSquadLast(int squadId) {
        byte[] key = new byte[5];
        key[0] = CATEGORY_SQUAD_LAST;
        System.arraycopy(Ints.toByteArray(squadId), 0, key, 1, 4);
        return key;
    }

    private byte[] keyOfCheckPoint(int squadId) {
        byte[] key = new byte[5];
        key[0] = CATEGORY_SQUAD_CHECKPOINT;
        System.arraycopy(Ints.toByteArray(squadId), 0, key, 1, 4);
        return key;
    }
}
