package org.axesoft.jaxos.logger;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.sleepycat.bind.EntryBinding;
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

    public BerkeleyDbAcceptorLogger(String path) {
        tryCreateDir(path);

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

    private void tryCreateDir(String path){
        File f = new File(path);
        if(f.exists()){
            if(!f.isDirectory()){
                throw new RuntimeException(path + " is not a directory");
            }
        } else {
            if(!f.mkdir()){
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

        DatabaseEntry valueEntry = new DatabaseEntry();
        promiseDataBinding.objectToEntry(promise, valueEntry);

        this.db.put(null, keyOfInt(instanceId), valueEntry);
    }

    @Override
    public Promise loadLastPromise(int squadId) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();

        Cursor cursor = null;
        Promise result = null;
        try {
            cursor = this.db.openCursor(null, null);

            if (cursor.getPrev(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                if(value.getSize() <= 1){
                    return null;
                }
                return (Promise) this.promiseDataBinding.entryToObject(value);
                //System.out.println(p.value.toStringUtf8());
                //System.out.println(p);
            }
            return null;
        }finally{
            if(cursor != null){
                cursor.close();
            }
        }

    }

    @Override
    public Promise loadPromise(int squadId, long instanceId) {
        DatabaseEntry key = keyOfInt(instanceId);
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

    private DatabaseEntry keyOfInt(long i){
        return new DatabaseEntry(Longs.toByteArray(i));
    }
}
