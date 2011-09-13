package fi.helsinki.cs.bsmr.master;

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

private static Logger logger = Util
		.getLoggerForClass(MasterWebSocketServlet.class);

@Override
public boolean checkOrigin(HttpServletRequest request, 
		String origin)
	{
	logger.fine("checkOrigin(): " + request + ", "+ origin);
	return super.checkOrigin(request, origin);
	}

@Override
protected void service(HttpServletRequest request, HttpServletResponse response)
		throws ServletException, IOException
	{
	logger.fine("service()");
	super.service(request, response);
	}

@Override
public WebSocket doWebSocketConnect(HttpServletRequest request,
		String service)
	{
	TimeContext.markTime();

	logger.finest("doWebSocketConnect()");

	MasterContext master = BSMRContext.getMaster(request.getServletContext());

	if (service != null)
		{
		if (service.equals("worker"))
			{
			return new Worker(master, request.getRemoteAddr());
			}

		if (service.equals("console"))
			{
			return new Console(master);
			}
		}
	return null; // TODO: how to communicate "no such service"?
	}

}
