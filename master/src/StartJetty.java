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

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;


import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.webapp.WebAppContext;



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