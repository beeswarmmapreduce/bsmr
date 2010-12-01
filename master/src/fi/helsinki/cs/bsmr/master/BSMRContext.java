package fi.helsinki.cs.bsmr.master;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import fi.helsinki.cs.bsmr.master.console.ConsoleNotifier;

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
		
		
		logger.fine("Creating Master");
		MasterImpl master = new MasterImpl();
		setMaster(sctx, master);
		
		
		logger.fine("Starting ConsoleNotifier thread");
		ConsoleNotifier cn = new ConsoleNotifier();
		cn.setMaster(master);
		cn.start();
		
		setConsoleNotifier(sctx, cn);
	}
	
	
	@Override
	public void contextDestroyed(ServletContextEvent evt) 
	{
		logger.info("Stopping BSMR Master");
		logger.fine("Stopping ConsoleNotifier thread");
		try {
			getConsoleNotifier(evt.getServletContext()).stop();;
		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, "Could not stop ConsoleNotifier thread!", e);
		}
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
