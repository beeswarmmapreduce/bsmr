package fi.helsinki.cs.bsmr.master;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketServlet;

public class MasterWebSocketServlet extends WebSocketServlet 
{
	private static final long serialVersionUID = 1L;
	
	private static Logger logger = Util.getLoggerForClass(MasterWebSocketServlet.class);
	
	// TODO: for testing purposes only. Change to private Master master afterwards
	//private Master master;
	public static Master master;
	
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
	public void init() throws ServletException
	{
		super.init();
		
		// TODO: for testing, uncomment later
		//master = new Master();
	}
	
	@Override
	protected WebSocket doWebSocketConnect(HttpServletRequest request, String service)
	{
		logger.finest("doWebSocketConnect()");
		
		if (service.equals("worker")) {
		
			return master.createWorker();
		}
		
		if (service.equals("console")) {
			// TODO: auth?
			return master.createConsole();
		}
		
		return null; // Seems there is no proper way to say "no such service"
	}

}
