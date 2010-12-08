package fi.helsinki.cs.bsmr.master;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import fi.helsinki.cs.bsmr.master.console.Console;
import fi.helsinki.cs.bsmr.master.console.ConsoleNotifier;

/**
 * An object responsible for starting and stopping the master. This object is registered as a listener class
 * so it receives events for when the server starts and stops this web application.
 * 
 * At startup this creates the MasterImpl, the ConsoleNotifier and Worker AsyncSender threads. The MasterImpl
 * and ConsoleNotifier are store in the ServletContext. At shutdown all threads are stopped and WebSocket clients
 * (Workers and Consoles) are disconnected.
 * 
 * 
 * @author stsavola
 *
 */
public class BSMRContext implements ServletContextListener
{
	private static Logger logger = Util.getLoggerForClass(BSMRContext.class);
	
	private static final String ATTRIBUTE_MASTER = "fi.helsinki.cs.bsmr.master.MasterInstance";
	private static final String ATTRIBUTE_CONSOLENOTIFIER = "fi.helsinki.cs.bsmr.master.console.ConsoleNotifierInstance";
	

	@Override
	public void contextInitialized(ServletContextEvent evt)
	{
		TimeContext.markTime();
		
		ServletContext sctx = evt.getServletContext();
		
		logger.info("Starting BSMR Master");
		
		
		logger.info("Creating Master");
		MasterImpl master = new MasterImpl();
		setMaster(sctx, master);
		
		
		logger.info("Starting ConsoleNotifier thread");
		ConsoleNotifier cn = new ConsoleNotifier();
		cn.setMaster(master);
		cn.start();
		setConsoleNotifier(sctx, cn);
		
		logger.info("Starting AsyncSender for workers");
		AsyncSender.getSender(master, "Worker AsyncSender");
	}
	
	
	@Override
	public void contextDestroyed(ServletContextEvent evt) 
	{
		logger.info("Stopping BSMR Master");
		MasterContext master = getMaster(evt.getServletContext());
	
		logger.info("Stopping ConsoleNotifier thread");
		try {
			getConsoleNotifier(evt.getServletContext()).stop();
		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, "Could not stop ConsoleNotifier thread!", e);
		}
		
		// NOTE: sync on master to prohibit disconnecting workers from modifying
		// the structures this block iterates over
		synchronized (master) {
			logger.info("Disconnecting all workers");
			for (Worker w : master.getWorkers()) {
				w.disconnect();
			}
			
			logger.info("Disconnecting all consoles");
			for (Console c : master.getConsoles()) {
				c.disconnect();
			}			
		}
		
		logger.info("Stopping all AsyncSenders");
		AsyncSender.stopAll();

		logger.info("BSMR Master stopped");
	}


	public static ConsoleNotifier getConsoleNotifier(ServletContext sctx)
	{
		return (ConsoleNotifier)sctx.getAttribute(ATTRIBUTE_CONSOLENOTIFIER);
	}
	
	public static void setConsoleNotifier(ServletContext sctx, ConsoleNotifier cn)
	{
		sctx.setAttribute(ATTRIBUTE_CONSOLENOTIFIER, cn);
	}

	public static MasterContext getMaster(ServletContext sctx)
	{
		return (MasterContext)sctx.getAttribute(ATTRIBUTE_MASTER);
	}
	
	public static void setMaster(ServletContext sctx, MasterContext master)
	{
		sctx.setAttribute(ATTRIBUTE_MASTER, master);
	}
	
}
