package fi.helsinki.cs.bsmr.master.console;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import fi.helsinki.cs.bsmr.master.MasterWebSocketServlet;
import fi.helsinki.cs.bsmr.master.TimeContext;

public class StatusServlet extends HttpServlet 
{
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException
	{
		TimeContext.markTime();
		
		resp.setContentType("text/plain");
		PrintWriter pw = resp.getWriter();
		
		ConsoleInformation ci = new ConsoleInformation(MasterWebSocketServlet.master);
		pw.println(ci.toJSONString());
		pw.close();
	}
}
