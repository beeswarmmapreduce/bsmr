package fi.helsinki.cs.bsmr.master.console;

/**
 * The MIT License
 * 
 * Copyright (c) 2010   Department of Computer Science, University of Helsinki
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * Author Sampo Savolainen
 *
 */

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
			
			ConsoleInformation ci = master.getConsoleInformation();
				
			synchronized (Console.class) { // See Console.onDisconnect()
				logger.finest("Informing all "+master.getConsoles().size()+" consoles");
				for (Console c : master.getConsoles()) {
					c.sendMessage(ci.toJSONString());
				}
			}

		}
	}
	
	public synchronized void sendUpdates()
	{
		this.notify();
	}

}
