package org.apache.cassandra.utils;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Responsible for stubbing out the System.exit() logic during unit tests.
 */
public class KillerForTests extends JVMStabilityInspector.Killer
{
    private boolean killed = false;

    @Override
    public void killCurrentJVM(Throwable t)
    {
        killed = true;
    }

    public boolean wasKilled()
    {
        return killed;
    }

    public void reset()
    {
        killed = false;
    }
}
