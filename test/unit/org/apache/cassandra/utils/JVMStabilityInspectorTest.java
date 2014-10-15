package org.apache.cassandra.utils;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JVMStabilityInspectorTest
{
    @Test
    public void testKill() throws Exception
    {
        JVMStabilityInspector.KillerForTests killerForTests = new JVMStabilityInspector.KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);

        Config.DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        Config.CommitFailurePolicy oldCommitPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new IOException());
            assertFalse(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new OutOfMemoryError());
            assertTrue(killerForTests.wasKilled());

            DatabaseDescriptor.setDiskFailurePolicy(Config.DiskFailurePolicy.die);
            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new FSReadError(new IOException(), "blah"));
            assertTrue(killerForTests.wasKilled());

            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.die);
            killerForTests.reset();
            JVMStabilityInspector.inspectCommitLogThrowable(new Throwable());
            assertTrue(killerForTests.wasKilled());
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(originalKiller);
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
            DatabaseDescriptor.setCommitFailurePolicy(oldCommitPolicy);
        }
    }

    @Test
    public void fileHandleTest() throws FileNotFoundException
    {
        JVMStabilityInspector.KillerForTests killerForTests = new JVMStabilityInspector.KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);

        try
        {
            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new SocketException("Should not fail"));
            assertFalse(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new FileNotFoundException("Also should not fail"));
            assertFalse(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new SocketException("Too many files open"));
            assertTrue(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectCommitLogThrowable(new FileNotFoundException("Too many files open"));
            assertTrue(killerForTests.wasKilled());
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }

    @Test
    public void socketTest()
    {
        JVMStabilityInspector.KillerForTests killerForTests = new JVMStabilityInspector.KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);

        ArrayList<ServerSocket> sockets;
        try
        {
            killerForTests.reset();
            sockets = new ArrayList<ServerSocket>();
            try
            {
                for (int i = 0; i < 20000; ++i) {
                    sockets.add(new ServerSocket(10000 + i));
                }
            }
            catch (Exception e)
            {
                System.err.println("Got an exception: " + e);
                JVMStabilityInspector.inspectThrowable(e);
            }
            finally
            {
                System.err.println("Number of sockets opened: " + sockets.size());
                for (ServerSocket s : sockets)
                {
                    s.close();
                }
            }
        }
        catch (Exception e)
        {
            System.err.println("HIT ERROR ON socketTest");
        }
        finally
        {
            assertTrue(killerForTests.wasKilled());
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }
}
