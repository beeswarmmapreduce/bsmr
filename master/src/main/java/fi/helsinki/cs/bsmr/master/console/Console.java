package fi.helsinki.cs.bsmr.master.console;

/**
 * The MIT License
 * 
 * Copyright (c) 2010-2014   Department of Computer Science, University of Helsinki
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

import java.util.Map;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.websocket.WebSocket;

import fi.helsinki.cs.bsmr.master.AsyncSender;
import fi.helsinki.cs.bsmr.master.JSON;
import fi.helsinki.cs.bsmr.master.Job;
import fi.helsinki.cs.bsmr.master.JobAlreadyRunningException;
import fi.helsinki.cs.bsmr.master.MasterContext;
import fi.helsinki.cs.bsmr.master.Message;
import fi.helsinki.cs.bsmr.master.TimeContext;
import fi.helsinki.cs.bsmr.master.Util;

/**
 * Console communication logic.
 * 
 * @author stsavola
 * 
 */
public class Console implements WebSocket, WebSocket.OnTextMessage
{
private static Logger logger = Util.getLoggerForClass(Console.class);

private Connection out;
private MasterContext master;

public Console(MasterContext master)
	{
	this.master = master;
	}

public void disconnect()
	{
	out.disconnect();
	}

@Override
public void onOpen(Connection out)
	{
	TimeContext.markTime();
	logger.fine("connected");
	this.out = out;

	// Immediately send out a status message
	sendStatus();

	master.addConsole(this);
	}

@Override
public void onClose(int i, String s)
	{
	TimeContext.markTime();
	master.removeConsole(this);
	AsyncSender.stopSenderIfPresent(this);

	logger.fine("disconnected");
	}


@SuppressWarnings("unchecked")
@Override
public void onMessage(String msg)
	{
	TimeContext.markTime();

	Map<Object, Object> request = (Map<Object, Object>) JSON.parse(msg);
	Object type = request.get("type");
	Map<Object, Object> payload = (Map<Object, Object>) request
			.get(Message.FIELD_PAYLOAD);

	boolean ok = false;

	synchronized (master)
		{

		if ("ADDJOB".equals(type))
			{
			logger.fine("Console adds a new job");
			if (logger.isLoggable(Level.FINEST))
				{
				logger.finest(" ADDJOB, payload: " + payload);
				}
			int jobId = addJob(payload);
			sendJobAdded(jobId);
			
			ok = true;
			}
		if ("REMOVEJOB".equals(type))
			{
			logger.fine("Console removes a job");
			if (logger.isLoggable(Level.FINEST))
				{
				logger.finest(" REMOVEJOB, payload: " + payload);
				}
			removeJob(payload);
			ok = true;
			}

		if (ok)
			{
			sendStatus();
			}
		else
			{
			logger.info("Unknown message from console: " + msg);
			}
		}
	}

/**
 * Handle a REMOVEJOB message from the console.
 * 
 * @param payload
 *            The payload part of the JSON message
 */
private void removeJob(Map<Object, Object> payload)
	{
	int jobId = Util.getIntFromJSONObject(payload.get("id"));

	Job toBeRemoved = master.getJobById(jobId);
	if (toBeRemoved == null)
		{
		logger.warning("Console tried to remove job " + jobId
				+ ", but no such job exists");
		return;
		}
	logger.info("Removing job: " + toBeRemoved);
	master.removeJob(toBeRemoved);
	}

/**
 * Handle an ADDJOB message from the console.
 * 
 * @param payload
 *            The payload part of the JSON message
 * @return 
 */
private int addJob(Map<Object, Object> payload)
	{
	int maptasks = Util.getIntFromJSONObject(payload
			.get(Message.FIELD_NUM_MAPTASKS));
	int reducetasks = Util.getIntFromJSONObject(payload
			.get(Message.FIELD_NUM_REDUCETASKS));
	int heartbeatTimeout = Util.getIntFromJSONObject(payload
			.get("heartbeatTimeout"));
	int acknowledgeTimeout = Util.getIntFromJSONObject(payload
			.get("progressTimeout"));
	Object code = payload.get(Message.FIELD_CODE);

	Job newJob = master.createJob(maptasks, reducetasks, heartbeatTimeout,
			acknowledgeTimeout, code);
	int jobId = newJob.getJobId();

	logger.info("Adding new job: " + newJob);
	try
		{
		master.queueJob(newJob);
		}
	catch (JobAlreadyRunningException jare)
		{
		logger
				.log(
						Level.SEVERE,
						"This should never happen! A newly created job was already running/had already been ran on the master?!?",
						jare);
		return jobId;
		}

	try
		{
		master.startNextJob();
		}
	catch (JobAlreadyRunningException jare)
		{
		/* NOP, because this just means that there is a job already running */
		}
		return jobId;
	}
/**
 * Create a new JobAdded message and send it to the console.
 * 
 * @see Console#sendJobAdded(int)
 */
public void sendJobAdded(int jobId)
	{
	String msg = "{\"type\":\"JOBADDED\",\"payload\":{\"id\":" + jobId + "}}";
	sendMessage(msg);
	}

/**
 * Create a new ConsoleInformation message and send it to the console.
 * 
 * @see Console#sendMessage(String)
 */
public void sendStatus()
	{
	ConsoleInformation ci = master.getConsoleInformation();
	String msg = ci.toJSONString();
	sendMessage(msg);
	}

/**
 * Send a message to the console. Sending the message is asynchronous: the
 * actual transmission is done by a AsyncSender (and thread) dedicated for this
 * console.
 * 
 * @param msg
 *            The message to send
 */
public void sendMessage(String msg)
	{
	AsyncSender sender = AsyncSender.getSender(this);
	sender.sendAsyncMessage(msg, out);
	}

}
