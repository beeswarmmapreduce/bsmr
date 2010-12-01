import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContext;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import fi.helsinki.cs.bsmr.master.Job;
import fi.helsinki.cs.bsmr.master.JobAlreadyRunningException;
import fi.helsinki.cs.bsmr.master.BSMRContext;
import fi.helsinki.cs.bsmr.master.MasterContext;

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
		
		
		try {
			server.start();
			
			ServletContext sctx = context.getServletContext();
			MasterContext master = BSMRContext.getMaster(sctx);
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			
			while( br.readLine() != null) {
				//Job j = Jmaster.createJob(1000, 30, 60000, 60000*60);
				Job j = master.createJob(3, 3, 60000, 60000*60);
				
				master.queueJob(j);
				try {
					master.startNextJob();
				} catch(JobAlreadyRunningException jare) {
					System.out.println("Still running the previous job");
				}
				
				// Send update to all consoles
				BSMRContext.getConsoleNotifier(sctx).sendUpdates();
				
			}
			
			
			server.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}