package org.lastbamboo.common.turn.http.server;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.ExecutorThreadModel;
import org.apache.mina.common.IoConnector;
import org.apache.mina.common.IoHandler;
import org.apache.mina.common.IoService;
import org.apache.mina.common.IoServiceConfig;
import org.apache.mina.common.IoServiceListener;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.ThreadModel;
import org.apache.mina.transport.socket.nio.SocketConnector;
import org.lastbamboo.common.tcp.frame.TcpFrame;
import org.lastbamboo.common.turn.client.TurnClientListener;
import org.lastbamboo.common.turn.client.TurnLocalIoHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that feeds data received in {@link TcpFrame}s to a locally-running
 * server.
 */
public class ServerDataFeeder implements TurnClientListener, IoServiceListener
    {

    private final Logger m_log = LoggerFactory.getLogger(getClass());
    
    private final Map<InetSocketAddress, IoSession> m_addressesToSessions =
        new ConcurrentHashMap<InetSocketAddress, IoSession>();

    private final InetSocketAddress m_serverAddress;
    
    /**
     * Creates a new {@link ServerDataFeeder} for feeding data to a local 
     * HTTP server.
     * 
     * @param serverAddress The address of the server to connect to.
     */
    public ServerDataFeeder(final InetSocketAddress serverAddress)
        {
        m_serverAddress = serverAddress;
        }
    
    public void onData(final InetSocketAddress remoteAddress, 
        final IoSession session, final byte[] data)
        {
        m_log.debug("Received data message");
        final IoSession localSession = 
            onRemoteAddressOpened(remoteAddress, session);
        final ByteBuffer dataBuf = ByteBuffer.wrap(data);
        
        localSession.write(dataBuf);
        m_log.debug("Local bytes written: {}", localSession.getWrittenBytes());
        }

    public IoSession onRemoteAddressOpened(
        final InetSocketAddress remoteAddress, final IoSession ioSession)
        {
        // We don't synchronize here because we're processing data from
        // a single TCP connection.
        if (m_addressesToSessions.containsKey(remoteAddress))
            {
            m_log.debug("Using existing local connection to: {}",remoteAddress);
            // This is the connection from the local proxy server to the 
            // local client.  So we're writing to our local server.
            return m_addressesToSessions.get(remoteAddress);
            }
        else
            {
            m_log.debug("Opening new local socket for remote address: {}", 
                remoteAddress);
            final IoConnector connector = new SocketConnector();
            connector.addListener(this);
            final ThreadModel threadModel = 
                ExecutorThreadModel.getInstance("TCP-TURN-Local-Socket");
            //connector.getDefaultConfig().setThreadModel(threadModel);
            connector.getDefaultConfig().setThreadModel(ThreadModel.MANUAL);
            final IoHandler ioHandler = 
                new TurnLocalIoHandler(ioSession, remoteAddress);
            
            final ConnectFuture ioFuture = 
                connector.connect(this.m_serverAddress, ioHandler);
            
            // We're just connecting locally, so it should be much quicker 
            // than this unless there's something wrong.
            ioFuture.join(6000);
            final IoSession session = ioFuture.getSession();
            if (!session.isConnected())
                {
                m_log.error("Could not connect to server: {}", 
                    this.m_serverAddress);
                return null;
                }
            else
                {
                m_log.debug("Connected to server: {}", this.m_serverAddress);
                this.m_addressesToSessions.put(remoteAddress, session);
                return session;
                }
            }
        }
    

    public void onRemoteAddressClosed(final InetSocketAddress remoteAddress)
        {
        if (!this.m_addressesToSessions.containsKey(remoteAddress))
            {
            // This would be odd -- could indicate someone fiddling
            // with our servers?
            m_log.warn("We don't know about the remote address: "+
                remoteAddress);
            }
        else
            {
            m_log.debug("Closing connection to local HTTP server...");
            final IoSession session = 
                this.m_addressesToSessions.remove(remoteAddress);
            
            // Stop the local session.  In particular, it the session
            // is in the middle of an HTTP transfer, this will stop
            // the HTTP server from sending more data to a host that's
            // no longer there on the other end.
            session.close();
            }
        }

    public void serviceActivated(final IoService service, 
        final SocketAddress serviceAddress, final IoHandler handler, 
        final IoServiceConfig config)
        {
        }

    public void serviceDeactivated(final IoService service, 
        final SocketAddress serviceAddress, final IoHandler handler, 
        final IoServiceConfig config)
        {
        }

    public void sessionCreated(final IoSession session)
        {
        }

    public void sessionDestroyed(final IoSession session)
        {
        // Inefficient, but not much to do.
        for (final Map.Entry<InetSocketAddress, IoSession> entry : 
            this.m_addressesToSessions.entrySet())
            {
            if (entry.getValue().equals(session))
                {
                this.m_addressesToSessions.remove(entry.getKey());
                return;
                }
            }
        m_log.warn("Did not find session!!");
        }

    public void close()
        {
        // Now close any of the local "proxied" sockets as well.
        final Collection<IoSession> sessions = 
            this.m_addressesToSessions.values();
        for (final IoSession curSession : sessions)
            {
            curSession.close();
            }
        this.m_addressesToSessions.clear();
        }
    }
