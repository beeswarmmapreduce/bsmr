package fi.helsinki.cs.bsmr.master.console;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.websocket.WebSocket;

import fi.helsinki.cs.bsmr.master.Master;
import fi.helsinki.cs.bsmr.master.TimeContext;
import fi.helsinki.cs.bsmr.master.Util;

public class Console implements WebSocket 
{
	private static Logger logger = Util.getLoggerForClass(Console.class);
	
	private Outbound out;
	private Master master;
	
	
	public Console(Master master)
	{
		this.master = master;
	}
	
	@Override
	public void onConnect(Outbound out) 
	{
		TimeContext.markTime();
		logger.fine("connected");
		this.out = out;
		
		// Immediately send out a status message
		sendStatus();
		
		master.addConsole(this);

	}

	@Override
	public void onDisconnect() 
	{
		master.removeConsole(this);
		TimeContext.markTime();
		logger.fine("disconnected");
	}

	@Override
	public void onMessage(byte frame, String msg) 
	{
		TimeContext.markTime();
		logger.fine("console says: "+msg);

	}

	@Override
	public void onMessage(byte arg0, byte[] arg1, int arg2, int arg3) 
	{
		TimeContext.markTime();
		
		// TODO: addjob, removejob (+startnext?)
		
		throw new RuntimeException("onMessage() byte format unsupported!");
	}

	@Override
	public void onFragment(boolean arg0, byte arg1, byte[] arg2, int arg3, int arg4) 
	{
		TimeContext.markTime();
		throw new RuntimeException("onFragment() byte format unsupported!");
	}
	
	public void sendStatus()
	{
		ConsoleInformation ci = new ConsoleInformation(master);
		sendStatus(ci);
	}
	
	public void sendStatus(ConsoleInformation ci)
	{
		String msg = ci.toJSONString();
		try {
			out.sendMessage(msg);
		} catch(IOException ie) {
			logger.log(Level.WARNING, "Exception while sending message to console, disconnecting",ie);
			out.disconnect();
		}
	}
	
}
