package org.axesoft.jaxos.logger;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import org.axesoft.jaxos.algo.AcceptorLogger;
import org.axesoft.jaxos.algo.CheckPoint;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class LevelDbAcceptorLogger implements AcceptorLogger {
    private static final Logger logger = LoggerFactory.getLogger(LevelDbAcceptorLogger.class);

    private static final byte CATEGORY_PROMISE = 2;
    private static final byte CATEGORY_SQUAD_LAST = 1;

    private DB db;
    private String path;

    public LevelDbAcceptorLogger(String path) {
        this.path = path;

        tryCreateDir(path);

        Options options = new Options().createIfMissing(true);

        try {
            db = Iq80DBFactory.factory.open(new File(path), options);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
    public void savePromise(int squadId, long instanceId, int proposal, ByteString value) {
        Promise promise = new Promise();
        promise.squadId = squadId;
        promise.instanceId = instanceId;
        promise.proposal = proposal;
        promise.value = value;

        byte[] data = toByteArray(promise);
        WriteOptions writeOptions = new WriteOptions().sync(true);

        WriteBatch writeBatch = db.createWriteBatch();
        byte[] promiseKey = keyOfInstanceId(squadId, instanceId);
        writeBatch.put(promiseKey, data);
        writeBatch.put(keyOfSquadLast(squadId), promiseKey);
        db.write(writeBatch, writeOptions);
    }

    @Override
    public Promise loadLastPromise(int squadId) {
        byte[] last = keyOfSquadLast(squadId);
        byte[] idx = db.get(last);
        if(idx == null){
            return null;
        }

        byte[] bx = db.get(idx);
        if(bx == null){
            return null;
        }

        return toEntity(bx);
    }

    @Override
    public Promise loadPromise(int squadId, long instanceId) {
        byte[] key = keyOfInstanceId(squadId, instanceId);
        byte[] bx = db.get(key);

        if(bx != null){
            return toEntity(bx);
        }
        return null;
    }

    @Override
    public void saveCheckPoint(CheckPoint checkPoint) {
        
    }

    @Override
    public CheckPoint loadLastCheckPoint(int squadId) {
        return null;
    }

    @Override
    public void close() {
        try {
            this.db.close();
        } catch (IOException e) {
            logger.error("Error when close db in " + this.path);
        }
    }

    public Promise toEntity(byte[] bytes) {
        DataInputStream is = new DataInputStream(new ByteArrayInputStream(bytes));
        Promise p = new Promise();
        try {
            p.squadId = is.readInt();
            p.instanceId = is.readLong();
            p.proposal = is.readInt();

            int size = is.readInt();
            byte[] bx = new byte[size];
            is.read(bx);
            p.value = ByteString.copyFrom(bx);

            return p;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public byte[] toByteArray(Promise p) {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(bs);
        try {
            os.writeInt(p.squadId);
            os.writeLong(p.instanceId);
            os.writeInt(p.proposal);
            os.writeInt(p.value.size());
            os.write(p.value.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return bs.toByteArray();
    }

    private byte[] keyOfInstanceId(int squadId, long i){
        byte[] key = new byte[13];
        key[0] = CATEGORY_PROMISE;
        System.arraycopy(Ints.toByteArray(squadId), 0, key, 1, 4);
        System.arraycopy(Longs.toByteArray(i), 0, key, 5, 8);
        return key;
    }

    private byte[] keyOfSquadLast(int squadId){
        byte[] key = new byte[5];
        key[0] = CATEGORY_SQUAD_LAST;
        System.arraycopy(Ints.toByteArray(squadId), 0, key, 1, 4);
        return key;
    }
}
