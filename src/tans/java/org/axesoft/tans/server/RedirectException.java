package org.axesoft.tans.server;

public class RedirectException extends  RuntimeException {
    private int serverId;
    public RedirectException(int serverId) {
        super();
        this.serverId = serverId;
    }

    public int getServerId(){
        return this.serverId;
    }
}
