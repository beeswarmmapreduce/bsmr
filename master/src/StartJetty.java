import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

public class StartJetty 
{
	public static void main(String[] args) 
	{
		Level logLevel = Level.FINE;
		
		Logger.getLogger("").setLevel(logLevel);
		for (Handler h : Logger.getLogger( "" ).getHandlers()) {
	        h.setLevel( logLevel );
	    }
		
		Server server = new Server(8080);

		WebAppContext context = new WebAppContext();
		context.setResourceBase("webapp");
		context.setDescriptor("webapp/WEB-INF/web.xml");
		context.setContextPath("/");
		context.setParentLoaderPriority(true);
		server.setHandler(context);
		
		

		try {
			server.start();
			server.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}