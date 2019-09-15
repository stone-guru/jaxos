package org.axesoft.tans.server;

public class RedirectException extends  RuntimeException {
    public RedirectException(String url) {
        super(url);
    }

    public String getUrl(){
        return super.getMessage();
    }
}
