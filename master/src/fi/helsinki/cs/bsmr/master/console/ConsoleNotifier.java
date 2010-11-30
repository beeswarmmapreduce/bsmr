package fi.helsinki.cs.bsmr.master.console;

import java.util.logging.Logger;

import fi.helsinki.cs.bsmr.master.Master;
import fi.helsinki.cs.bsmr.master.TimeContext;
import fi.helsinki.cs.bsmr.master.Util;

public class ConsoleNotifier implements Runnable
{
	private static Logger logger = Util.getLoggerForClass(ConsoleNotifier.class);
	
	public static final long TIME_BETWEEN_NOTIFYS = 5000;
	
	private Thread thread;
	private boolean running;
	
	private Master master;
	
	public ConsoleNotifier()
	{
		thread = null;
	}
	
	public void setMaster(Master m)
	{
		this.master = m;
	}
	
	public Master getMaster()
	{
		return master;
	}
	
	public synchronized void start()
	{
		if (thread != null) throw new IllegalThreadStateException("ConsoleNotifier already started");
		
		running = true;
		thread = new Thread(this);
		thread.start();
	}
	
	public synchronized void stop() throws InterruptedException 
	{
		running = false;
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
					logger.info("Waiting for notification");
					this.wait(ConsoleNotifier.TIME_BETWEEN_NOTIFYS);
					
					wasInterrupted = false;
				} catch(InterruptedException ie) {
					wasInterrupted = true;
				}
			
			}
			
			// If interrupted, or no master => exit
			if (wasInterrupted || master == null) continue;
			
			TimeContext.markTime();
			ConsoleInformation ci;
			
			synchronized (master) {
				ci = new ConsoleInformation(master);	
			}
			
			synchronized (Console.class) { // See Console.onDisconnect()
				logger.info("Informing all "+master.getConsoles().size()+" consoles");
				for (Console c : master.getConsoles()) {
					c.sendStatus(ci);
				}
			}

		}
	}
	
	public synchronized void sendUpdates()
	{
		this.notify();
	}

}
