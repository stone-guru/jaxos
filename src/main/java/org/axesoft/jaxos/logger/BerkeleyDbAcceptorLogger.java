package org.axesoft.jaxos.logger;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.*;
import org.axesoft.jaxos.algo.AcceptorLogger;

import java.io.File;

public class BerkeleyDbAcceptorLogger implements AcceptorLogger {

    public static class PromiseTupleBinding extends TupleBinding {

        @Override
        public Object entryToObject(TupleInput input) {
            Promise p = new Promise();
            p.squadId = input.readInt();
            p.instanceId = input.readLong();
            p.proposal = input.readInt();

            int size = input.readInt();
            byte[] bx = new byte[size];
            input.read(bx);
            p.value = ByteString.copyFrom(bx);

            return p;
        }

        @Override
        public void objectToEntry(Object object, TupleOutput to) {
            Promise p = (Promise)object;
            to.writeInt(p.squadId);
            to.writeLong(p.instanceId);
            to.writeInt(p.proposal);
            to.writeInt(p.value.size());
            to.write(p.value.toByteArray());
        }
    }

    private Database db;
    private Environment dbEnv;
    private StoredClassCatalog classCatalog;
    private EntryBinding promiseDataBinding;
    private TransactionConfig txconfig;

    public BerkeleyDbAcceptorLogger(String path) {
        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(true);
        config.setTransactional(true);
        this.dbEnv = new Environment(new File(path), config);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        this.db = this.dbEnv.openDatabase(null, "jaxosdb", dbConfig);

        this.classCatalog = new StoredClassCatalog(this.db);
        this.promiseDataBinding = new PromiseTupleBinding();
    }

    @Override
    public void saveLastPromise(int squadId, long instanceId, int proposal, ByteString value) {
        Promise promise = new Promise();
        promise.squadId = squadId;
        promise.instanceId = instanceId;
        promise.proposal = proposal;
        promise.value = value;

        DatabaseEntry valueEntry = new DatabaseEntry();
        promiseDataBinding.objectToEntry(promise, valueEntry);

        this.db.put(null, new DatabaseEntry(Longs.toByteArray(instanceId)), valueEntry);
    }

    @Override
    public Promise loadLastPromise(int squadId) {
        DatabaseEntry key = keyOfInt(squadId);
        DatabaseEntry value = new DatabaseEntry();

        OperationStatus status = this.db.get(null, key, value, LockMode.DEFAULT);
        if(status == OperationStatus.SUCCESS) {
            return (Promise) this.promiseDataBinding.entryToObject(value);
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        try{
            this.db.close();
            this.dbEnv.close();
        }catch (DatabaseException e){
            e.printStackTrace(System.err);
        }
    }

    private DatabaseEntry keyOfInt(int i){
        return new DatabaseEntry(Ints.toByteArray(i));
    }
}
