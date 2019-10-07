package org.axesoft.jaxos.logger;

import org.axesoft.jaxos.algo.AcceptorLogger;
import org.axesoft.jaxos.algo.CheckPoint;
import org.axesoft.jaxos.algo.Event;
import org.axesoft.jaxos.algo.InstanceValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileAcceptorLogger implements AcceptorLogger {
    private static final Logger logger = LoggerFactory.getLogger(FileAcceptorLogger.class);

    private String dbPath;
    private DataOutputStream[] streams;

    public FileAcceptorLogger(String dbPath, int squadNumber) {
        this.dbPath = dbPath;
        this.streams = new DataOutputStream[squadNumber];
        try {
            for (int i = 0; i < this.streams.length; i++) {
                this.streams[i] = new DataOutputStream(new FileOutputStream(dbPath + "/" + logNameOf(i)));
            }
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private String logNameOf(int squadId) {
        return "s" + squadId + "-instances.jil";
    }

    /**
     * @param squadId
     * @param instanceId
     * @param proposal
     * @param value
     *
     * message section is :
     *  8 instance id
     *  4 message size, exclude instance id and this size field
     *  4 proposal,
     *  4 message type,
     *  4 value size
     *  x values
     *  total size is 24 + value size
     *  message size is 12 + value size
     */
    @Override
    public void savePromise(int squadId, long instanceId, int proposal, Event.BallotValue value) {
        DataOutputStream os = streams[squadId];
        synchronized (os) {
            int valueSize = value.content().size();

            try {
                os.writeLong(instanceId);
                os.writeInt(valueSize + 12);
                os.writeInt(proposal);
                os.write(value.type().code());
                os.write(valueSize);
                os.write(value.content().toByteArray());
                os.flush();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    @Override
    public InstanceValue loadLastPromise(int squadId) {
        return null;
    }

    @Override
    public InstanceValue loadPromise(int squadId, long instanceId) {
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

    }

    public InstanceValue toEntity(byte[] bytes) {
//        DataInputStream is = new DataInputStream(new ByteArrayInputStream(bytes));
//        InstanceValue p = new InstanceValue();
//        try {
//            p.squadId = is.readInt();
//            p.instanceId = is.readLong();
//            p.proposal = is.readInt();
//
//            int size = is.readInt();
//            byte[] bx = new byte[size];
//            is.read(bx);
//            p.value = ByteString.copyFrom(bx);
//
//            return p;
//        }
//        catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        return null;
    }
}
