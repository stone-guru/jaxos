package org.axesoft.jaxos.algo;

public class ProposalNumHolder {
    private int serverId;
    private int rangeLength;

    public ProposalNumHolder(int serverId, int rangeLength) {
        this.serverId = serverId;
        this.rangeLength = rangeLength;
    }

    public int getProposal0(){
        if(Math.random() >= 0.5){
            return this.serverId + rangeLength/2;
        }
        return this.serverId;
    }

    public int nextProposal(int proposal){
        return ((proposal / this.rangeLength) + 1) * this.rangeLength + this.serverId;
    }
}
