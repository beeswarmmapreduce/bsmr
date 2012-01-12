function addFlashBinary()
	{
	var swfVersionStr = "10.2.0";
    
	var xiSwfUrlStr = "playerProductInstall.swf";
	var flashvars = {};
	var params = {};
	params.quality = "high";
	params.bgcolor = "#ffffff";
	params.allowscriptaccess = "sameDomain";
	params.allowfullscreen = "true";
	var attributes = {};
	attributes.id = "JavascriptInterface";
	attributes.name = "JavascriptInterface";
	attributes.align = "middle";
	
	document.getElementById("bsmrplugin").innerHTML="";
	swfobject.embedSWF("lib/JavascriptInterface.swf", "bsmrplugin", "523", "188", swfVersionStr, xiSwfUrlStr, flashvars, params, attributes);
	swfobject.createCSS("#bsmrplugin", "display:block;text-align:left;");	
	}

function getFlashMovie(movieName) 
	{   
	var isIE = navigator.appName.indexOf("Microsoft") != -1;   
	return (isIE) ? window[movieName] : document[movieName];  
	}  