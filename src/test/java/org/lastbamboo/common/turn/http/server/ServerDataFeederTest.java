package org.lastbamboo.common.turn.http.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.ArrayUtils;
import org.littleshoot.mina.common.IoSession;
import org.littleshoot.mina.common.WriteFuture;
import org.junit.Test;
import org.lastbamboo.common.stun.stack.message.turn.SendIndication;
import org.lastbamboo.common.turn.http.server.stubs.IoSessionStub;
import org.littleshoot.util.ByteBufferUtils;
import org.littleshoot.util.NetworkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for the class that feeds data to a server by opening new connections. 
 */
public class ServerDataFeederTest
    {
    
    private final Logger m_log = LoggerFactory.getLogger(getClass());
    
    private ServerSocket m_dataServerSocket;

    private final AtomicBoolean m_unexpectedDataOnServer =
        new AtomicBoolean(false);

    private final AtomicInteger m_dataServerMessagesReceived = 
        new AtomicInteger(0);

    private volatile int m_dataServerMessagesValidated = 0;

    private final AtomicBoolean m_socketServingFailed = 
        new AtomicBoolean(false);

    private final AtomicInteger m_dataServerSockets = new AtomicInteger(0);
    
    private final AtomicBoolean m_dataServerStarted = new AtomicBoolean(false);
    private final AtomicBoolean m_stopDataServer = new AtomicBoolean(false);
    
    private final AtomicReference<byte[]> m_dataOnServer =
        new AtomicReference<byte[]>();
    
    private final AtomicInteger m_dataMessagesWrittenFromServer =
        new AtomicInteger(0);
    
    private static final byte[] DATA = createData();

    // We limit this data size because most protocols passing data
    // cannot be larger than 0xffff -- they use 2 bytes of the size.
    private static final int DATA_SIZE = 0xffff - 2000;

    private static final int NUM_REMOTE_HOSTS = 10;

    private static final int NUM_MESSAGES_PER_HOST = 10;

    @Test public void testFeedServer() throws Exception
        {
        final InetSocketAddress serverAddress = startThreadedServer();
        synchronized (m_dataServerStarted)
            {
            if (!m_dataServerStarted.get())
                {
                m_dataServerStarted.wait(3000);
                }
            }
        final ServerDataFeeder feeder = new ServerDataFeeder(serverAddress);
        
        final Map<InetSocketAddress, Collection<ByteBuffer>>  addressesToMessages = 
            new HashMap<InetSocketAddress, Collection<ByteBuffer>>();
        final Collection<SendIndication> messages = 
            new LinkedList<SendIndication>();
        
        final AtomicInteger totalDataSent = new AtomicInteger(0);
        final AtomicInteger totalDataReceived = new AtomicInteger(0);
        final IoSession session = new IoSessionStub()
            {
            @Override
            public WriteFuture write(final Object message)
                {
                final SendIndication di = (SendIndication) message;
                final InetSocketAddress address = di.getRemoteAddress();
                if (addressesToMessages.containsKey(address))
                    {
                    final Collection<ByteBuffer> bufs = 
                        addressesToMessages.get(address);
                    addRaw(bufs, di);
                    }
                else
                    {
                    final Collection<ByteBuffer> bufs = 
                        new LinkedList<ByteBuffer>();
                    addressesToMessages.put(address, bufs);
                    addRaw(bufs, di);
                    }
                messages.add(di);
                if (messages.size() == NUM_REMOTE_HOSTS)
                    {
                    synchronized (messages)
                        {
                        messages.notify();
                        }
                    }
                return null;
                }

            // This method is required to extract the data from the TCP frames 
            // to verify it matches the original data sent (since the server
            // just echoes everything it sees).
            private void addRaw(final Collection<ByteBuffer> bufs, 
                final SendIndication si)
                {
                final byte[] rawData = si.getData();
                final byte[] noFrame = 
                    ArrayUtils.subarray(rawData, 2, rawData.length);
                
                totalDataReceived.addAndGet(noFrame.length);
                bufs.add(ByteBuffer.wrap(noFrame));
                
                
                synchronized (totalDataReceived)
                    {
                    if (totalDataReceived.get() == totalDataSent.get())
                        {
                        totalDataReceived.notify();
                        }
                    }
                }
            };
        
        for (int j = 0; j < NUM_MESSAGES_PER_HOST; j++)
            {
            for (int i = 0; i < NUM_REMOTE_HOSTS; i++)
                {
                final InetSocketAddress remoteAddress = 
                    new InetSocketAddress("44.52.67."+(1+i), 4728+i);
                feeder.onRemoteAddressOpened(remoteAddress, session);
                feeder.onData(remoteAddress, session, DATA);
                totalDataSent.addAndGet(DATA.length);
                }
            }
        
        synchronized (m_dataServerSockets)
            {
            if (m_dataServerSockets.get() < NUM_REMOTE_HOSTS)
                {
                m_dataServerSockets.wait(6000);
                }
            }
        
        assertEquals(NUM_REMOTE_HOSTS, m_dataServerSockets.get());
        
        synchronized (totalDataReceived)
            {
            if (totalDataReceived.get() < totalDataSent.get())
                {
                totalDataReceived.wait(6000);
                }
            }

        assertEquals(totalDataSent.get(), totalDataReceived.get());
        
        // The messages received should be TURN Send Indications that wrap
        // TCP Frames.  They're already placed in the appropriate buckets for
        // each remote host.  Now we just need to verify the data.
        
        for (final Map.Entry<InetSocketAddress, Collection<ByteBuffer>> entry : 
            addressesToMessages.entrySet())
            {
            verifyData(entry.getValue());
            }
        }
    
    private void verifyData(final Collection<ByteBuffer> value)
        {
        m_log.debug("Combining "+value.size()+" messages");
        final ByteBuffer combined = ByteBufferUtils.combine(value);
        final byte[] data = new byte[combined.capacity()];
        combined.get(data);
        assertEquals("Lengths not equal", DATA.length*NUM_MESSAGES_PER_HOST, 
            data.length);
        
        // The server should have responded to all the messages that were sent,
        // so we'll have a bunch of messages combined here.
        for (int i = 0; i < NUM_MESSAGES_PER_HOST; i++)
            {
            final int index = i * DATA.length;
            final byte[] subArray = 
                ArrayUtils.subarray(data, index, index+DATA.length);
            assertTrue("Data not equal", Arrays.equals(DATA, subArray));
            m_log.debug("One success!!!");
            }
        }

    private static byte[] createData()
        {
        // Just fill it with data with a clear order that can be screwed up
        // in the case of threading errors or anything else.
        final byte[] data = new byte[DATA_SIZE];
        for (int i = 0; i < 127 ; i++)
            {
            data[i] = (byte) (i % 127);
            }
        return data;
        }
    
    private InetSocketAddress startThreadedServer() throws UnknownHostException
        {
        final InetSocketAddress serverAddress = 
            new InetSocketAddress(NetworkUtils.getLocalHost(), 7896);
        final Runnable runner = new Runnable()
            {
            public void run()
                {
                try
                    {
                    startServer(serverAddress);
                    }
                catch (final Throwable t)
                    {
                    fail("Bad, bad server");
                    }
                }
            };
        final Thread turnThread = 
            new Thread(runner, "TURN-Test-Data-Server-Thread");
        turnThread.setDaemon(true);
        turnThread.start();
        return serverAddress;
        }

    private void startServer(final InetSocketAddress serverAddress) throws Exception
        {
        this.m_dataServerSocket = new ServerSocket(serverAddress.getPort());
        synchronized (m_dataServerStarted)
            {
            m_dataServerStarted.set(true);
            m_dataServerStarted.notify();
            }
        
        while(!this.m_dataServerSocket.isClosed())
            {
            final Socket client;
            try
                {
                client = m_dataServerSocket.accept();
                m_dataServerSockets.incrementAndGet();
                synchronized (m_dataServerSockets)
                    {
                    m_dataServerSockets.notify();
                    }
                }
            catch (final SocketException se)
                {
                m_log.debug("Socket closed");
                assertTrue(m_stopDataServer.get());
                // This will happen when the server socket's closed.
                break;
                }
            m_log.debug("Got data socket number: "+m_dataServerSockets);
            final Runnable serverSocketRunner = new Runnable()
                {
                public void run()
                    {
                    try
                        {
                        serveDataSocket(client);
                        }
                    catch (final Throwable t)
                        {
                        m_socketServingFailed.set(true);
                        }
                    }
                };
            final Thread serverSocketThread = 
                new Thread(serverSocketRunner, "Server-Socket-Serving-Thread");
            serverSocketThread.setDaemon(true);
            serverSocketThread.start();
            }
        }
    
    private void serveDataSocket(final Socket client) throws IOException
        {
        final OutputStream os  = client.getOutputStream();
        final InputStream is = client.getInputStream();
        while (!m_stopDataServer.get())
            {
            final byte[] request = new byte[DATA_SIZE];
            final ByteBuffer buf = ByteBuffer.wrap(request);
            final ReadableByteChannel channel = Channels.newChannel(is);
            while (buf.hasRemaining())
                {
                channel.read(buf);
                }
            m_dataOnServer.set(request);
            
            m_dataServerMessagesReceived.incrementAndGet();
            synchronized (m_dataServerMessagesReceived)
                {
                m_dataServerMessagesReceived.notify();
                }
            m_log.debug("Client socket "+client.getPort()+" has received "+
                m_dataServerMessagesReceived.get()+" messages...");
            if (!Arrays.equals(DATA, request))
                {
                m_log.error("Bad data received on DATA server!!");
                m_unexpectedDataOnServer.set(true);
                }
            else
                {
                m_dataServerMessagesValidated++;
                m_log.debug("Data server has validated "+m_dataServerMessagesValidated+
                    " messages...");
                }
            
            // Now, write the response and make sure the remote host ultimately
            // gets the raw data back.
            os.write(DATA);
            os.flush();
            m_dataMessagesWrittenFromServer.incrementAndGet();
            synchronized (m_dataMessagesWrittenFromServer)
                {
                m_dataMessagesWrittenFromServer.notify();
                }
            }
        }
    }
