package fi.helsinki.cs.bsmr.master.console;

import java.util.logging.Logger;

import fi.helsinki.cs.bsmr.master.MasterContext;
import fi.helsinki.cs.bsmr.master.TimeContext;
import fi.helsinki.cs.bsmr.master.Util;

/**
 * A thread which sends out periodical notifications to all consoles.
 * 
 * @author stsavola
 *
 */
public class ConsoleNotifier implements Runnable
{
	private static Logger logger = Util.getLoggerForClass(ConsoleNotifier.class);
	
	public static final long TIME_BETWEEN_NOTIFYS = 5000;
	
	private Thread thread;
	private boolean running;
	
	private MasterContext master;
	
	public ConsoleNotifier()
	{
		thread = null;
	}
	
	public void setMaster(MasterContext m)
	{
		this.master = m;
	}
	
	public MasterContext getMaster()
	{
		return master;
	}
	
	public synchronized void start()
	{
		if (thread != null) throw new IllegalThreadStateException("ConsoleNotifier already started");
		
		running = true;
		thread = new Thread(this, "ConsoleNotifier thread");
		thread.start();
	}
	
	public void stop() throws InterruptedException 
	{
		running = false;
		synchronized (this) {
			this.notify();
		}
		thread.interrupt();
		thread.join();
	}
	
	@Override
	public void run() 
	{
		while (running) {
		
			boolean wasInterrupted;
			
			synchronized (this) {
				
				try {
					logger.finest("Waiting for notification");
					this.wait(ConsoleNotifier.TIME_BETWEEN_NOTIFYS);
					
					wasInterrupted = false;
				} catch(InterruptedException ie) {
					wasInterrupted = true;
				}
			
			}
			
			// If interrupted, or no master => exit
			if (wasInterrupted || master == null) continue;
			
			TimeContext.markTime();
			
			String msg;
			
			synchronized (master) {
				ConsoleInformation ci = new ConsoleInformation(master);
				msg = ci.toJSONString();
			}
			
			synchronized (Console.class) { // See Console.onDisconnect()
				logger.finest("Informing all "+master.getConsoles().size()+" consoles");
				for (Console c : master.getConsoles()) {
					c.sendMessage(msg);
				}
			}

		}
	}
	
	public synchronized void sendUpdates()
	{
		this.notify();
	}

}
