package com.mq.proxy.core.protocol;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RemotingCommandVersionTest {

    @After
    public void tearDown() {
        System.clearProperty(RemotingCommand.REMOTING_VERSION_KEY);
        RemotingCommand.resetConfigVersionForTest();
    }

    @Test
    public void testCreateRequestCommandUsesDefaultRemotingVersion() {
        System.clearProperty(RemotingCommand.REMOTING_VERSION_KEY);
        RemotingCommand.resetConfigVersionForTest();

        RemotingCommand cmd = RemotingCommand.createRequestCommand(100, null);

        assertEquals(RemotingCommand.DEFAULT_REMOTING_VERSION, cmd.getVersion());
    }

    @Test
    public void testCreateRequestCommandUsesConfiguredRemotingVersion() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");
        RemotingCommand.resetConfigVersionForTest();

        RemotingCommand cmd = RemotingCommand.createRequestCommand(101, null);

        assertEquals(2333, cmd.getVersion());
    }
}
