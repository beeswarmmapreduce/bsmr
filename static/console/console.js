var COMPLETE = '&#10003;';
//var DELETED = '&#128293;';
var RUNNING = '&#127939;';
var QUEUED = '&#8987;';

var previoustab;
var currenttab;

var completed = [];
var running = [];
var queued = [];

var ourjobs = [];

var tabs = {};
var ws;

var DEFAULTMASTER = "ws://localhost:8080/bsmr";
var masterurl = DEFAULTMASTER;

function creategrid(id, length, cellstatus) {
	if (typeof(cellstatus) == typeof(undefined)) {
		cellstatus = 'unknown';
	}
	var LINELEN = 50;
	var i = 0;
	html = '<table class="grid" id="grid-' + id + '">';
	while (i < length) {
		html += '<tr>';
			for (var j = 0; j < LINELEN; j++) {
				var classes = cellstatus;
				if (i >= length) {
					classes = 'disabled';
				}
				html += '<td title="' + i + ', ' + classes + '" class="' + classes + '" id="grid-' + id + '-cell-' + i + '"></td>';
			i++;
		}
		html += '</tr>';
	}
	html += '</table>';
	return html;
}

function clearjobs() {
	for (var i in completed) {
		job = completed[i];
		removejob(job.jobId);
	}
}

function nukejobs() {
	for (var i in completed) {
		job = completed[i];
		removejob(job.jobId);
	}
	for (var i in running) {
		job = running[i];
		removejob(job.jobId);
	}
	for (var i in queued) {
		job = queued[i];
		removejob(job.jobId);
	}
}

function updatenum(id, num) {
	document.getElementById(id).innerHTML = num;
}

function updateworkers(workers) {
	var idhead = '<th class="number">worker</th>';
	var timehead = '<th class="number">connect time</th>';
	var statushead = '<th class="status">status</th>';
	var urlhead = '<th colspan="2" class="url">inter url</th>';
	var html = '<tr>' + idhead + timehead + statushead + urlhead + '</tr>';
	for (var id in workers) {
		var worker = workers[id];
		var idcell = '<td class="number">' + id + '</td>';
		var timecell = '<td class="number">' + worker.connectTime+ '</td>';
		var statuscell = '<td class="status">' + worker.status + '</td>';
		var urlcell = '<td class="url">' + worker.url + '</td>';
		html += '<tr>' + idcell + timecell + statuscell + urlcell + '</tr>';
	}
	document.getElementById('workers').innerHTML = html;
}

function newsymbol(id, statussymbol) {
	var classes = ['symbol'];
	if (ourjobs.indexOf(id) > -1 || id == '+') {
		classes.push('our');
	}
	if (statussymbol == RUNNING) {
		classes.push('running');
	}
	var classatt = 'class="' + classes.join(' ') + '"';
	
	return '<span ' + classatt + ' id="status-' + id + '">' + statussymbol + '</span>';
}

function newtitle(id) {
	var title = id;
	if (id == '+') {
		title = 'new';
	}
	var classes = ['title'];
	var classatt = 'class="' + classes.join(' ') + '"';
	return '<span ' + classatt + '">' + title + '</span>';
}

function tabhead(id, statussymbol, hidetitle) {
	if (id == '+') {
		statussymbol = '+';
	}
	var classes = [];
	var inline = [];
	if (typeof(statussymbol) != typeof(undefined)) {
		symbol = newsymbol(id, statussymbol);
		inline.push(symbol);
	}
	if (!hidetitle) {
		var title = newtitle(id);
		inline.push(title);
	}
	if (id == currenttab) {
		classes.push('selected');
	}
	var idatt = 'id="head-' + id + '"';
	var classatt = 'class="' + classes.join(' ') + '"';
	var titleatt = 'title="' + id + '"';
	var clickcode = "switchtab('" + id + "');";
	var clickatt = 'onClick="' + clickcode + '"';
	var atts = [idatt, classatt, titleatt, clickatt].join(' ');
	var inner = inline.join(' ');
	return '<td ' + atts + '>' + inner + '</td>';
}

function updatenums(workers) {
	var wc = 0;
	for (undefined in workers) {
		wc += 1;
	}
	updatenum('wc', wc);
	var cj = completed.length;
	var rj = running.length;
	var qj = queued.length;
	updatenum('cj', cj);
	updatenum('rj', rj);
	updatenum('qj', qj);
	updatenum('jc', cj + rj + qj);
}

function updateheads(hidetitle) {
	var html = '';
	for (var i in completed) {
	  	var job = completed[i];
		html += tabhead(job.jobId, COMPLETE, hidetitle);
		}
	for (var i in running) {
		var job = running[i];
		html += tabhead(job.jobId, RUNNING);
	}
	for (var i in queued) {
		var job = queued[i];
		html += tabhead(job.jobId, QUEUED, hidetitle);
	}
	html += tabhead('+');
	var headholder = document.getElementById('heads');
	headholder.innerHTML = html;
}

function jobcount() {
	return completed.length + running.length + queued.length;
}

function panel(content) {
	return '<div class="panel">' + content + '</div>';
}

function tools(title, tools) {
	if (typeof(tools) == typeof(undefined)) {
	tools = '<input type="button" value="dummy" style="visibility: hidden;" />';
	}
	return '<table class="tools"><tr><td>' + title + '</td><td>' + tools + '</td></tr></table>';
}

function jobtab(id) {
	var title = '<h3>job ' + id + '</h3>';
	var remove = '<input type="button" value="remove job" onclick="removejob(' + id + ')"/>';
	var jtools = tools(title, remove);
	var maph = '<h4>mapping</h4>';
	var mapg = '<div id="map-' + id + '"></div>';
	var map = tools(maph) + mapg;
	var redh = '<h4>reducing</h4>';
	var redg = '<div id="red-' + id + '"></div>';
	var red = tools(redh) + redg;
	var codeh = '<h4>code</h4>';
	var codef = '<textarea disabled="disabled" id="code-' + id + '"></textarea>';
	var code = tools(codeh) + codef;
	return jtools + panel(map + red + code);
}

function setcell(gridid, cellid, cellstatus, label) {
	var id = 'grid-' + gridid + '-cell-' + cellid;
	var cell = document.getElementById(id);
	if (cell != null) {
		cell.setAttribute('title', cellid + ', ' + cellstatus);
		cell.setAttribute('class', cellstatus);
		cell.innerHTML = label;
	}
}

function setcellsbynodelists(gridid, nodelists, cellstatus) {
	for (var cellid in nodelists) {
		var nodes = nodelists[cellid];
		var label = nodes.length;
		setcell(gridid, cellid, cellstatus, label);
	}
}

function updateparts(id, parts) {
	if (typeof(parts) == typeof(undefined)) {
		return;
	}
	for (var cellstatus in parts) {
		var nodelists = parts[cellstatus];
		setcellsbynodelists(id, nodelists, cellstatus);
	}
}

function updatesplits(id, splits) {
	updateparts('map-' + id, splits);
}

function updatebuckets(id, buckets) {
	updateparts('red-' + id, buckets);
}

function updatemap(jobId, finished, M, splits) {
	var mapstat = undefined;
	if (finished) {
		mapstat = 'gone'; // map results are discarded after the job is finished
	}
	var mgrid = creategrid('map-' + jobId, M, mapstat);
	document.getElementById('map-' + jobId).innerHTML = mgrid;
	if (finished != true) {
		updatesplits(jobId, splits);
	}
}

function updatered(jobId, finished, R, buckets) {
	var redstat = undefined;;
	if (finished) {
		redstat = 'done'; // reduce results remain around
	}
	var rgrid = creategrid('red-' + jobId, R, redstat);
	document.getElementById('red-' + jobId).innerHTML = rgrid;

	if (finished != true) {
		updatebuckets(jobId, buckets);
	}
}

function updatecode(job) {
	var id = job.jobId;
	var code = document.getElementById('code-' + id);
	code.innerHTML = job.code;
}

function updatetab(job, buckets, splits) {
	var jobId = job.jobId;
	var finished = job.finished;
	var M = job.M;
	var R = job.R;
	var tab = gettab(jobId);
	if (tab == null) {
		newtab(jobId);
	}
	updatemap(jobId, finished, M, splits);
	updatered(jobId, finished, R, buckets);
	updatecode(job);
	tabs[jobId] = true;
}

function updatetabs(buckets, splits) {
	for (var i in tabs) {
		tabs[i] = false;
	}
	for (var i in completed) {
		var job = completed[i];
		job.finished = true;
		updatetab(job);
	}
	for (var i in running) {
		var job = running[i];
		updatetab(job, buckets, splits);
	}
	for (var i in queued) {
	  	var job = queued[i];
		updatetab(job);
	}
	for (var i in tabs) {
		if (tabs[i] == false) {
			destroytab(i);
		}
	}
}

function switchlatest() {
	var switchto = '+';
	if (running.length > 0) {
		var oldest = running[0];
		switchto = oldest.jobId;
	} else if (completed.length > 0) {
		var newest = completed[completed.length - 1];
		switchto = newest.jobId;
	} else if (queued.length > 0) {
		var oldest = queued[0];
		switchto = oldest.jobId;
	}
	switchtab(switchto);
}

function updatefocus() {
	var autof = document.getElementById('autof').checked;
	if(autof == true) {
		switchlatest();
	}
	document.getElementById();

	if(gethead(currenttab) == null) {
		switchlatest();
	}
}

function autoclear() {
	var autoc = document.getElementById('autoc').checked;
	if(autoc == true) {
		clearjobs();
	}
}

function updatejobs(msg) {
	completed = [];
	running = [];
	queued = [];
	var history = msg.jobHistory;
	for (var i in history) {
		var job = history[i];
		completed.push(job);
	}
	var current = [];
	if (typeof(msg.job) != typeof(undefined)) {
		current.push(msg.job);
	}
	for (var i in current) {
		var job = current[i];
		if (job.finished == true) {
			completed.push(job);
		} else {
			running.push(job);
		}
	}
	var queue = msg.jobQueue;
	for (var i in queue) {
		var job = queue[i];
		queued.push(job);
	}
}

function msgparse(msg) {
	if (msg.type == 'STATUS') {
		var payload = msg.payload;
		var workers = payload.workers;
		var buckets = payload.buckets;
		var splits = payload.splits;
		updatejobs(payload);
		updateworkers(workers);
		updatenums(workers);
		updatetabs(buckets, splits);
		var hidenames = jobcount() > 10;
		updateheads(hidenames);
		updatefocus();
		autoclear();
	} else if(msg.type == 'JOBADDED') {
		var payload = msg.payload;
		var jobId = payload.id;
		ourjobs.push(jobId);
	} else {
		console.log('unknown msg type ' + msg.type);
	}
}

function defaultcode() {
	var request = new XMLHttpRequest();
	request.open('GET', 'defaultjob.js', false);
	request.send(null);
	if (request.status === 200) {
		return request.responseText;
	}
}

function updatemfoo() {
	var cells = parseInt(document.getElementById('M').value, 10);
	document.getElementById('newmfoo').innerHTML = creategrid('mfoo', cells);
}

function updaterfoo() {
	var cells = parseInt(document.getElementById('R').value, 10);
	document.getElementById('newrfoo').innerHTML = creategrid('rfoo', cells);
}

function joblauncher() {
	var defaultm = 1000;
	var defaultr = 1000;
	var mgrid = creategrid('mfoo', defaultm);
	var rgrid = creategrid('rfoo', defaultr);
	var title = '<h3><em>new job</em></h3>';
	var add = '<input id="jobaddbutton" type="button" value="add job" onClick="addjob();" />';
	var jtools = tools(title, add);
	var maph = '<h4>mapping</h4>';
	var mapc = '<input id="M" type="text" value="' + defaultm + '" onchange="updatemfoo();" /> mappers';
	var mapg = '<div id="newmfoo">' + mgrid + '</div>';
	var map = tools(maph, mapc) + mapg;
	var redh = '<h4>reducing</h4>';
	var redc = '<input id="R" type="text" value="' + defaultr + '" onchange="updaterfoo();" /> reducers';
	var redg ='<div id="newrfoo">' + rgrid + '</div>';
	var red = tools(redh, redc) + redg;
	var codeh = '<h4>code</h4>';
	var codef = '<textarea id="jobcode">' + defaultcode() + '</textarea>';
	var code = tools(codeh) + codef;
	return jtools + '<div class="panel">' + map + red + code + '</div>';
}

function addjob() {
	var payload = {};
	payload.code = document.getElementById("jobcode").value;
	payload.R = document.getElementById("R").value;
	payload.M = document.getElementById("M").value;
	payload.heartbeatTimeout = "60000";
	payload.progressTimeout = "300000";
	var msg = {};
	msg.type = "ADDJOB";
	msg.payload = payload;
	var jsonmsg = JSON.stringify(msg);
	ws.send(jsonmsg);
}

function removejob(id) {
	var payload = {};
	payload.id = id;
	var msg = {};
	msg.type = "REMOVEJOB";
	msg.payload = payload;
	var jsonmsg = JSON.stringify(msg);
	ws.send(jsonmsg);
}

function gethead(id) {
	return document.getElementById('head-' + id);
}

function gettab(id) {
	return document.getElementById('tab-' + id);
}

function switchtab(id) {
	if (id == currenttab) {
		return;
	}
	next = gettab(id);
	if (next == null) {
		return;
	}
	next.setAttribute("style", "display: block;");
	nexthead = gethead(id);
	if (nexthead != null) {
		nexthead.setAttribute("class", "selected");
	}
	ctab = gettab(currenttab);
	if (ctab != null) {
		ctab.removeAttribute("style");
	}
	chead = gethead(currenttab);
	if (chead != null) {
		chead.removeAttribute("class");
	}
	previoustab = currenttab;
	currenttab = id;
}

function destroytab(id) {
	tab = gettab(id);
	if (tab == null) {
		return;
	}
	var tabholder = document.getElementById('tabholder');
	tabholder.removeChild(tab);
	if(previoustab == id) {
		previoustab = undefined;
	}
	if(currenttab == id) {
		currenttab = undefined;
	}
	delete(tabs[id]);
}

function newtab(id) {
	var code;
	if (id == '+') {
		code = joblauncher();
	} else {
		code = jobtab(id);
	}

	tab = gettab(id);
	if (tab != null) {
		return;
	}
	if (typeof(statussymbol) == typeof(undefined)) {
		statussymbol = '';
	}
	var tabholder = document.getElementById('tabholder');
	tabholder.innerHTML += '<div id="tab-' + id + '">' + code + '</div>';
}

function jobarea() {
	var title = '<h2>jobs</h2>';
	var nuke = '<input type="button" value="nuke all" onclick="nukejobs();" />';
	var clear = '<input type="button" value="clear old" onclick="clearjobs();" />';
	var aclear = '<input id="autoc" type="checkbox" /> autoclear';
	var afollow = '<input id="autof" type="checkbox" checked="checked" /> autofollow';
	var controls = nuke + clear + aclear + afollow;
	var toolb = tools(title, controls);
	var tabview = '<table><tr id="heads"></tr></table><div id="tabholder"></div>';
	return panel(toolb + tabview);
}

function workerarea() {
	var title = '<h2>workers</h2>';
	var toolb = tools(title);
	var workertable = '<table id="workers"></table>';
	return panel(toolb + workertable);
}

function createnum(id, value) {
	return '<em><span id="' + id + '">' + value + '</span></em>';
}

function createbody() {
	var title = '<h1>Bee Swarm MapReduce</h1>';
	var url = '<em id="murl">-</em>';
	var states = '(' + createnum('cj', 0) + ' completed, ' + createnum('qj', 0) + ' queued, and ' + createnum('rj', 0) + ' running)';
	var connect = 'Connected to ' + url + ' with ' + createnum('wc', 0) + ' available workers and ' + createnum('jc', 0) + ' jobs ' + states + '.';
	var ja = jobarea();
	var wa = workerarea();
	return tools(title, connect) + ja + wa;
}

function parseParams() {
    var query = window.location.href.split('?')[1];
    var defs = [];
    if (query !== undefined) {
        defs = query.split('&');
    }
    var params = {};
    for (var i in defs) {
        var parts = defs[i].split("=");
        params[parts[0]] = parts[1];
    }
    return params;
}

function connectToMaster(masterurl) {
	var murl = document.getElementById('murl');
	ws = new WebSocket(masterurl, "console");
	ws.onmessage = function(e) { msgparse(JSON.parse(e.data)); };
	ws.onopen = function(e) { murl.innerHTML = masterurl; };	
}

function init() {
	var params = parseParams();
	var target = masterurl;
	var masterd = params.master;
	if (typeof(masterd) != typeof(undefined)) {
		target = 'ws://' + masterd + '/bsmr';}
	
	document.getElementById('body').innerHTML = createbody();
	newtab('+');
	updateheads();
	switchlatest();
	connectToMaster(target);
}

