package fi.helsinki.cs.bsmr.master;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketServlet;

import fi.helsinki.cs.bsmr.master.console.Console;

/**
 * The WebSocketServlet which creates Worker and Console end points.
 * 
 * @author stsavola
 *
 */
public class MasterWebSocketServlet extends WebSocketServlet 
{
	private static final long serialVersionUID = 1L;
	
	private static Logger logger = Util.getLoggerForClass(MasterWebSocketServlet.class);
	
	@Override
	protected String checkOrigin(HttpServletRequest request, String host, String origin)
	{
		logger.fine("checkOrigin(): "+request+", "+host+", "+origin);
		return super.checkOrigin(request, host, origin);
	}
	
	@Override
	protected void service(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException
	{
		logger.fine("service()");
		super.service(request, response);
	}
	
	@Override
	protected WebSocket doWebSocketConnect(HttpServletRequest request, String service)
	{
		TimeContext.markTime();
		
		logger.finest("doWebSocketConnect()");
	
		MasterContext master = BSMRContext.getMaster(request.getServletContext());
		
		if (service != null) {
			if (service.equals("worker")) {
				return master.createWorker(request.getRemoteAddr());
			}
			
			if (service.equals("console")) {
				return new Console(master);
			}
		}
		return null; // TODO: how to communicate "no such service"?
	}

}
