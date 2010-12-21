import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContext;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.webapp.WebAppContext;

import fi.helsinki.cs.bsmr.master.Job;
import fi.helsinki.cs.bsmr.master.JobAlreadyRunningException;
import fi.helsinki.cs.bsmr.master.BSMRContext;
import fi.helsinki.cs.bsmr.master.MasterContext;


public class StartJetty 
{
	public static void main(String[] args) throws Exception
	{		
		setLogLevel(Level.FINE);
		
		Server server = new Server(8080);

        ContextHandlerCollection multiContext = new ContextHandlerCollection();
        multiContext.addHandler(createMasterWebApp());
        multiContext.addHandler(createWorkerWebApp());
        multiContext.addHandler(createConsoleWebApp());
        
        server.setHandler(multiContext);
		
		try {
			server.start();
			server.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void setLogLevel(Level logLevel) 
	{
		Logger.getLogger("").setLevel(logLevel);
		for (Handler h : Logger.getLogger( "" ).getHandlers()) {
	        h.setLevel( logLevel );
	    }
	}

	private static WebAppContext createMasterWebApp() 
	{
		WebAppContext context = new WebAppContext();
		context.setResourceBase("master/webapp");
		context.setDescriptor("master/webapp/WEB-INF/web.xml");
		context.setContextPath("/");
		context.setParentLoaderPriority(true);
		return context;
	}
	
	private static WebAppContext createWorkerWebApp()
	{
		WebAppContext context = new WebAppContext();
		context.setResourceBase("worker");
		context.setContextPath("/worker");
		return context;
	}
	
	private static WebAppContext createConsoleWebApp()
	{
		WebAppContext context = new WebAppContext();
		context.setResourceBase("console");
		context.setContextPath("/console");
		return context;
	}
}