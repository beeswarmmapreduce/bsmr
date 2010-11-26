package fi.helsinki.cs.bsmr.master.console;

import java.util.logging.Logger;

import org.eclipse.jetty.websocket.WebSocket;

import fi.helsinki.cs.bsmr.master.TimeContext;
import fi.helsinki.cs.bsmr.master.Util;
import fi.helsinki.cs.bsmr.master.WorkerStore;

public class Console implements WebSocket 
{
	private static Logger logger = Util.getLoggerForClass(Console.class);
	
	private WorkerStore workerStore;
	private Outbound out;
	
	public Console(WorkerStore workerStore)
	{
		this.workerStore = workerStore;
	}
	
	@Override
	public void onConnect(Outbound out) 
	{
		TimeContext.markTime();
		logger.fine("connected");
		this.out = out;
	}

	@Override
	public void onDisconnect() 
	{
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
		
		
		throw new RuntimeException("onMessage() byte format unsupported!");
	}

	@Override
	public void onFragment(boolean arg0, byte arg1, byte[] arg2, int arg3, int arg4) 
	{
		TimeContext.markTime();
		throw new RuntimeException("onFragment() byte format unsupported!");
		
	}

}
