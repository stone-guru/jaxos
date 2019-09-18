package org.axesoft.jaxos.algo;

import static com.google.common.base.Preconditions.checkArgument;

public class ProposalNumHolder {
    private int serverId;
    private int rangeLength;

    public ProposalNumHolder(int serverId, int rangeLength) {
        checkArgument(serverId > 0 && serverId < rangeLength, "server id %d beyond range");
        this.serverId = serverId;
        this.rangeLength = rangeLength;
    }

    public int getProposal0(){
        if(Math.random() >= 0.5){
            return this.serverId + rangeLength;
        }
        return this.serverId;
    }

    public int nextProposal(int proposal){
        final int r = this.rangeLength << 1;
        return ((proposal / r) + 1) * r + this.serverId;
    }
}
