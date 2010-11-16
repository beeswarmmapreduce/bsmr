import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import fi.helsinki.cs.bsmr.master.Job;
import fi.helsinki.cs.bsmr.master.Master;
import fi.helsinki.cs.bsmr.master.MasterWebSocketServlet;

public class StartJetty 
{
	public static void main(String[] args) throws Exception
	{
		Level logLevel = Level.FINE;
		
		Logger.getLogger("").setLevel(logLevel);
		for (Handler h : Logger.getLogger( "" ).getHandlers()) {
	        h.setLevel( logLevel );
	    }
		
		Server server = new Server(8080);

		WebAppContext context = new WebAppContext();
		context.setResourceBase("master/webapp");
		context.setDescriptor("master/webapp/WEB-INF/web.xml");
		context.setContextPath("/");
		context.setParentLoaderPriority(true);
		server.setHandler(context);
		
		
		// Add a false master and a made up job
		Master master = new Master();
		Job j = Job.createJob(1000, 30, 60000, 60000*60);
		master.queueJob(j);
		master.startNextJob();
		
		MasterWebSocketServlet.master = master;

		try {
			server.start();
			server.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}