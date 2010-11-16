package fi.helsinki.cs.bsmr.master;

import java.util.logging.Logger;

import org.eclipse.jetty.websocket.WebSocket;

public class Console implements WebSocket 
{
	private static Logger logger = Util.getLoggerForClass(Console.class);
	
	private Master master;
	private Outbound out;
	
	public Console(Master master)
	{
		this.master = master;
	}
	
	@Override
	public void onConnect(Outbound out) 
	{
		logger.fine("connected");
		this.out = out;
	}

	@Override
	public void onDisconnect() 
	{
		logger.fine("disconnected");
	}

	@Override
	public void onMessage(byte frame, String msg) 
	{
		logger.fine("console says: "+msg);

	}

	@Override
	public void onMessage(byte arg0, byte[] arg1, int arg2, int arg3) 
	{
		throw new RuntimeException("onMessage() byte format unsupported!");
	}

}
