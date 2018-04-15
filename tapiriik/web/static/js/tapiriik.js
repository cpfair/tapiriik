// Blegh, I should really get around to using django-compressor
(function(e){if(typeof define==="function"&&define.amd){define(["jquery"],e)}else{e(jQuery)}})(function(e){function n(e){return u.raw?e:encodeURIComponent(e)}function r(e){return u.raw?e:decodeURIComponent(e)}function i(e){return n(u.json?JSON.stringify(e):String(e))}function s(e){if(e.indexOf('"')===0){e=e.slice(1,-1).replace(/\\"/g,'"').replace(/\\\\/g,"\\")}try{e=decodeURIComponent(e.replace(t," "));return u.json?JSON.parse(e):e}catch(n){}}function o(t,n){var r=u.raw?t:s(t);return e.isFunction(n)?n(r):r}var t=/\+/g;var u=e.cookie=function(t,s,a){if(s!==undefined&&!e.isFunction(s)){a=e.extend({},u.defaults,a);if(typeof a.expires==="number"){var f=a.expires,l=a.expires=new Date;l.setTime(+l+f*864e5)}return document.cookie=[n(t),"=",i(s),a.expires?"; expires="+a.expires.toUTCString():"",a.path?"; path="+a.path:"",a.domain?"; domain="+a.domain:"",a.secure?"; secure":""].join("")}var c=t?undefined:{};var h=document.cookie?document.cookie.split("; "):[];for(var p=0,d=h.length;p<d;p++){var v=h[p].split("=");var m=r(v.shift());var g=v.join("=");if(t&&t===m){c=o(g,s);break}if(!t&&(g=o(g))!==undefined){c[m]=g}}return c};u.defaults={};e.removeCookie=function(t,n){if(e.cookie(t)===undefined){return false}e.cookie(t,"",e.extend({},n,{expires:-1}));return!e.cookie(t)}})

var csrftoken = $.cookie('csrftoken');

function csrfSafeMethod(method) {
	// these HTTP methods do not require CSRF protection
	return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
}
$.ajaxSetup({
	crossDomain: false, // obviates need for sameOrigin test
	beforeSend: function(xhr, settings) {
		if (!csrfSafeMethod(settings.type)) {
			xhr.setRequestHeader("X-CSRFToken", csrftoken);
		}
	}
});

tapiriik = typeof(tapiriik) == "undefined" ? {} : tapiriik;
tapiriik.PreviousURLComponents = [];

tapiriik.Init = function(){
	// I swear, getting this to happen automatically in CSS or Django templating is nearly impossible.
	$(".controls").each(function(){if ($(".row",this).length>2) {$(this).addClass("multi");}});
	// ...
	$(".syncButton").click(tapiriik.ImmediateSyncRequested);
	$("a.authDialog").click(tapiriik.AuthDialogLinkClicked);
	$(".service a.configDialog").click(tapiriik.ConfigDialogLinkClicked);
	$(".service a.deauthDialog").click(tapiriik.DeauthDialogLinkClicked);
	$("button.clearException").click(tapiriik.ClearExceptionLinkClicked);

	if (tapiriik.User !== undefined){
		if (tapiriik.User.ConnectedServicesCount > 1){
			tapiriik.UpdateCountdownTimer = setInterval(tapiriik.UpdateSyncCountdown, 60000);
			tapiriik.RefreshCountdownTimer = setInterval(tapiriik.RefreshSyncCountdown, 500);
			tapiriik.UpdateSyncCountdown();
		}
	}
	$(".logo").click(function(e){
		if (e.shiftKey){
			tapiriik.ShowDebugInfo();
			return false;
		}
	});

	$(".paymentForm").hide();
	$(".paymentButton").show();
	$(".paymentButton").click(function(){
		$(".paymentButton").slideUp();
		$(".paymentForm").slideDown();
		return false;
	});
	$(".reclaimButton").click(tapiriik.PaymentReclaimDialogLinkClicked);
	$(".promoButton").click(tapiriik.PaymentPromoDialogLinkClicked);

	if (tapiriik.User !== undefined) {
		for (var i in tapiriik.ServiceInfo) {
			if (tapiriik.ServiceInfo[i].Connected && tapiriik.ServiceInfo[i].RequiresConfiguration && !tapiriik.ServiceInfo[i].Configured){
				tapiriik.ActivateSetupDialog(i);
				break; // we can nag them again if there's >1
			}
			if (tapiriik.User.AutoSyncActive && tapiriik.ServiceInfo[i].Connected && tapiriik.ServiceInfo[i].HasExtendedAuth && !tapiriik.ServiceInfo[i].PersistedExtendedAuth){
				if (!$.cookie("no-remember-details-nag-" + i)){
					tapiriik.ActivateRememberDetailsDialog(i);
					break;
				}
			}
		}
	}

	$.address.change(tapiriik.AddressChanged);
	tapiriik.AddressChanged();

	// Detect TZ.
	if (tapiriik.User && !tapiriik.User.Substitute){
		var tz = jstz.determine().name();
		if (tz != tapiriik.User.Timezone){
			$.post("/account/settz", {timezone: tz});
			tapiriik.User.Timezone = tz;
		}
	}

	// Info tips
	$(".infotip").each(function(){
		$("<span>").text("x").addClass("close").appendTo(this).click(function(){
			$(this).parent().slideUp();
			if ($.cookie("infotip_hide")) {
				$.cookie("infotip_hide", $.cookie("infotip_hide") + "," + $(this).parent().attr("id"), {expires: 3650});
			} else {
				$.cookie("infotip_hide", $(this).parent().attr("id"));
			}
		});
	});

	//setInterval(tapiriik.CycleLogo, 60 * 1000);
};

tapiriik.AddressChanged=function(){
	var components = $.address.pathNames();
	var unchangedDepth = 0;
	for (var i = 0; i < tapiriik.PreviousURLComponents.length; i++) {
		if (i>components.length-1 || components[i] != tapiriik.PreviousURLComponents[i]){
			break;
		}
		unchangedDepth = i+1;
	}
	tapiriik.PreviousURLComponents = components;
	if (components[0]=="auth") {
		tapiriik.OpenAuthDialog(components[1]);
		return;
	} else if (components[0]=="remember-details") {
		tapiriik.OpenRememberDetailsDialog(components[1]);
		return;
	} else if (components[0]=="disconnect") {
		tapiriik.OpenDeauthDialog(components[1]);
		return;
	} else if (components[0]=="payments" && components[1]=="claim"){
		tapiriik.OpenPaymentReclaimDialog();
		return;
	} else if (components[0]=="payments" && components[1]=="claimed"){
		tapiriik.OpenPaymentReclaimCompletedDialog();
		return;
	} else if (components[0]=="payments" && components[1]=="promo"){
		tapiriik.OpenPaymentPromoDialog();
		return;
	} else if (components[0]=="configure") {
		if (components[1]=="dropbox" && components[2]=="setup"){
			if (tapiriik.ServiceInfo.dropbox.AccessLevel == "full"){
				if (unchangedDepth<=2) {
					tapiriik.DropboxBrowserPath = tapiriik.ServiceInfo.dropbox.Config.SyncRoot;
					$.address.value("configure/dropbox/setup" + tapiriik.DropboxBrowserPath); // init directory, meh
					tapiriik.OpenDropboxConfigDialog();
				} else {
					tapiriik.DropboxBrowserPath = "/" + components.slice(3).join("/");
					tapiriik.PopulateDropboxBrowser();
				}
			} else {
				tapiriik.OpenDropboxConfigDialog();
			}
			return;
		}
		tapiriik.DoDismissServiceDialog();
		tapiriik.OpenServiceConfigPanel(components[1]);
		return;
	} else if (components[0] == "dropbox") {
		if (components[1] == "info"){
			tapiriik.OpenDropboxInfoDialog();
			return;
		}
	} else if (components[0] == "settings") {
		tapiriik.OpenSyncSettingsDialog();
	} else {
		tapiriik.CloseSyncSettingsDialog();
	}
	tapiriik.DoDismissServiceDialog();
	tapiriik.DoDismissConfigPanel();
};

tapiriik.SaveConfig = function(svcId, config, callback) {
	$.post("/configure/save/"+svcId, {"config": JSON.stringify(tapiriik.ServiceInfo[svcId].Config)},function(){
		$.address.value("");
		window.location.reload();
	});

};

tapiriik.AuthDialogLinkClicked = function(e){
	var svcId = $(this).attr("service");
	if (tapiriik.ServiceInfo[svcId].NoFrame){
		return; // prevents super-annoying redirect loop if you back up from the auth page
	}
	$.address.value("auth/" + svcId);
	return false;
};

tapiriik.ConfigDialogLinkClicked = function(e){
	$.address.value("configure/"+$(this).attr("service"));
	return false;
};

tapiriik.DeauthDialogLinkClicked = function(e){
	$.address.value("disconnect/"+$(this).attr("service"));
	return false;
};

tapiriik.IFrameOAuthReturn=function(success){
	if (success){
		$.address.value("");
		window.location.reload();
	} else {
		$.address.value("");
	}
};


tapiriik.OpenAuthDialog = function(svcId){
	var mode = tapiriik.ServiceInfo[svcId].AuthenticationType;
	var contents;

	if (mode == "oauth"){
		if (tapiriik.ServiceInfo[svcId].NoFrame){ // this should never happen, but in case someone curious tries the URL
			window.location = tapiriik.ServiceInfo[svcId].AuthorizationURL;
			contents = $("<div><h1>Weeeeee</h1>(redirecting you right now)</div>");
		} else {
			contents = $("<iframe>").attr("src",tapiriik.ServiceInfo[svcId].AuthorizationURL).attr("id",svcId);
		}
	} else if (mode == "direct") {
		contents = tapiriik.CreateDirectLoginForm(svcId);
	}
	tapiriik.CreateServiceDialog(svcId, contents);
};

tapiriik.OpenRememberDetailsDialog = function(svcId){
	var mode = tapiriik.ServiceInfo[svcId].AuthenticationType;
	var contents;

	contents = $("<form><center><p>If you don't let tapiriik <b>remember your <span class=\"service-name\"></span> credentials</b>,<br/> you'll need to come back and re-enter them every hour.</p><button id=\"remember-nack\" class=\"cancel\">No thanks</button><button id=\"remember-ack\">Remember my login</button><br/><p>(either way, your other accounts will not be affected)</p></center></form>");
	$(".service-name", contents).text(tapiriik.ServiceInfo[svcId].DisplayName);

	$("#remember-ack", contents).click(function(){
		$.post("/auth/persist-ajax/" + svcId, function(){
			tapiriik.ServiceInfo[svcId].PersistedExtendedAuth = true;
			$.address.value("");
		});
		return false;
	});

	$("#remember-nack", contents).click(function(){
		$.cookie("no-remember-details-nag-" + svcId, "1", {expires: 1});
		$.address.value("");
		return false;
	});
	tapiriik.CreateServiceDialog(svcId, contents);
};

tapiriik.ActivateRememberDetailsDialog = function(svcId){
	$.address.value("remember-details/" + svcId);
};

tapiriik.OpenDeauthDialog = function(svcId){
	var form = $("<form><center><button id=\"disconnect\" class=\"delete\">Disconnect</button><button id=\"cancel\" class=\"cancel\">Never mind</button></center></form><h2>(nothing will be deleted)</h2>");
	form.bind("submit", function() {return false;});
	$("#disconnect", form).click(function(){
		if (tapiriik.DeauthPending !== undefined) return false;
		tapiriik.DeauthPending = true;
		$("#disconnect", form).addClass("disabled");
		$.ajax({url:"/auth/disconnect-ajax/"+svcId,
				type:"POST",
				success: function(){
					$.address.value("");
					window.location.reload();
				},
				error: function(data){
					alert("Error in disconnection: " + $.parseJSON(data.responseText).error+"\n Please contact me ASAP");
					tapiriik.DeauthPending = undefined;
					$("#disconnect", form).removeClass("disabled");
				}});
		return false;
	});

	$("#cancel", form).click(function(){
		history.back();
	});

	tapiriik.CreateServiceDialog(svcId, form);
};

tapiriik.CreateDirectLoginForm = function(svcId){
	var form = $("<form novalidate><div class=\"error\" id=\"login-fail\">There was a problem logging you in</div><div class=\"error\" id=\"login-error\">There was a system error :(</div><label for=\"email\">Email/Username</label><input autofocus type=\"email\" id=\"email\"/><label for=\"password\">Password</label><input type=\"password\" id=\"password\"><br/><span class=\"persist-controls\"><input type=\"checkbox\" id=\"persist\"/><label for=\"persist\">Save these details</label><br/></span><center><button type=\"submit\" >Log in</button></center></form>");
	if (!tapiriik.ServiceInfo[svcId].UsesExtendedAuth){
		$(".persist-controls",form).hide();
	}
	var loginPending = false;
	form.bind("submit", function(){
		if (loginPending) return false;
		loginPending=true;
		$("button",form).addClass("disabled");
		$.post("/auth/login-ajax/"+svcId,{username:$("#email",form).val(),password:$("#password",form).val(), persist:$("#persist",form).is(":checked")?"1":null}, function(data){
			if (data.success) {
				$.address.value("");
				window.location.reload();
			} else {
				if (typeof data.result === 'object' && data.result.type == "renew_password" && svcId == "garminconnect") {
					alert("You need to visit connect.garmin.com directly to fix a problem with your account.\n\nOnce you're done, try logging in again.");
				}
				if (typeof data.result === 'object' && data.result.type == "locked" && svcId == "garminconnect") {
					alert("If you entered your Garmin Connect username instead of your email, try using your email. If that doesn't work, visit connect.garmin.com to double-check your login.\n\nOnce you're done, try logging in again.");
				}
				if (typeof data.result === 'object' && data.result.type == "non_athlete_account" && svcId == "trainingpeaks") {
					alert("It looks like you used a TrainingPeaks Coach account - you'll have to sign in with your individual account to continue.");
				}
				$(".error", form).hide();
				$("#login-fail", form).show();
				$("button",form).removeClass("disabled");
				loginPending = false;
			}
		}, "json").fail(function(){
			$(".error", form).hide();
			$("#login-error", form).show();
		});
		return false;
	});
	return form;
};

tapiriik.ActivateConfigDialog = function(svcId){
	$.address.value("configure/" + svcId);
};

tapiriik.ActivateSetupDialog = function(svcId){
	$.address.value("configure/" + svcId + "/setup");
};

tapiriik.OpenServiceConfigPanel = function(svcId){
	if ($(".service#"+svcId+" .flowConfig").length>0) return; //it's already open
	tapiriik.DoDismissConfigPanel();
	var configPanel = $("<form class=\"flowConfig\"><h1>Options</h1><div class=\"configSection\"><h2>send activities to...</h2><table class=\"serviceTable\"></table></div><div class=\"configSection\" id=\"sync_private_section\"><input type=\"checkbox\" id=\"sync_private\"/><label for=\"sync_private\">Sync private activities</label></div><div class=\"configSection\" id=\"auto_pause_section\"><input type=\"checkbox\" id=\"auto_pause\"/><label for=\"auto_pause\">Simulate auto-pause</label></div><span class=\"fineprint\">Settings will take effect at next sync</span><button id=\"setup\">Setup</button><button id=\"save\">Save</button><button id=\"disconnect\" class=\"delete\">Disconnect</button></form>");
	for (var i in tapiriik.ServiceInfo) {
		if (i == svcId || !tapiriik.ServiceInfo[i].Connected || !tapiriik.ServiceInfo[i].ReceivesActivities) continue;
		var destSvc = tapiriik.ServiceInfo[i];
		var destRow = $("<tr><td><input type=\"checkbox\" class=\"to\" id=\"flow-to-" + i +"\"/></td><td><label for=\"flow-to-" + i + "\">" + tapiriik.ServiceInfo[i].DisplayName + "</label></td></tr>");
		if (tapiriik.ServiceInfo[svcId].BlockFlowTo.indexOf(i) < 0) {
			$("input.to", destRow).attr("checked","checked");
		}
		$("input", destRow).attr("service", i);
		$("table", configPanel).append(destRow);
	}
	if (svcId == "strava" || svcId == "runkeeper" || svcId == "sporttracks" || svcId == "garminconnect" || svcId == "motivato" || svcId == "velohero")
	{
		if (tapiriik.ServiceInfo[svcId].Config.sync_private)
		{
			$("#sync_private", configPanel).attr("checked", 1);
		}
	} else {
		$("#sync_private_section", configPanel).hide();
	}
	if (svcId == "runkeeper")
	{
		if (tapiriik.ServiceInfo[svcId].Config.auto_pause)
		{
			$("#auto_pause", configPanel).attr("checked", 1);
		}
	} else {
		$("#auto_pause_section", configPanel).hide();
	}
	$("button#save", configPanel).click(function(){
		if ($(this).hasClass("disabled")) return;
		$(this).addClass("disabled");

		tapiriik.ServiceInfo[svcId].Config.sync_private = $("#sync_private", configPanel).is(":checked");
		tapiriik.ServiceInfo[svcId].Config.auto_pause = $("#auto_pause", configPanel).is(":checked");

		var flowFlags = {"forward":[]};
		var flags = $("input[type=checkbox]", configPanel);
		for (var i = 0; i < flags.length; i++) {
			if ($(flags[i]).is(":checked")){
				flowFlags.forward.push($(flags[i]).attr("service"));
			}
		}
		$.post("/configure/flow/save/"+svcId, {"flowFlags": JSON.stringify(flowFlags)}, function(){
			$.post("/configure/save/" + svcId, {"config": JSON.stringify(tapiriik.ServiceInfo[svcId].Config)}, function(){
				$.address.value("");
			setTimeout(function(){window.location.reload();}, 400); //would be possible to resolve the changes in JS to avoid a reload, I'll leave that for later
			})
		});

		return false;
	});
	$("button#disconnect", configPanel).click(function(){
		$.address.value("disconnect/"+svcId);
		return false;
	});

	if (tapiriik.ServiceInfo[svcId].Configurable){
		$("button#setup", configPanel).click(function(){
			tapiriik.ActivateSetupDialog(svcId);
			return false;
		});
	} else {
		$("button#setup", configPanel).hide();
	}


	tapiriik.CreateConfigPanel(svcId, configPanel);
};



tapiriik.OpenDropboxConfigDialog = function(){
	var configPanel = $("<form class=\"dropboxConfig\"><h1>Set Up Dropbox Sync</h1>\
		<label>Select sync folder</label><div id=\"folderList\"></div>\
		<div id=\"folderStackOuter\"><span id=\"syncLocationPreamble\">Will sync to</span> <span id=\"folderStack\"></span></div>\
		<div id=\"reauth_up\">Want to sync to a different location? You'll need to <a href=\"/auth/redirect/dropbox/full\">authorize tapiriik to access your entire Dropbpx folder</a>.</div>\
		<div id=\"reauth_down\">Don't want tapiriik to have full access to your Dropbox? <a href=\"/auth/redirect/dropbox\">Restrict tapiriik to <tt>/Apps/tapiriik/</tt></a>.</div>\
		<label>Upload new activites as:</label>\
			<input type=\"text\" id=\"filename\" style=\"width:300px\"/>\
			<input type=\"hidden\" id=\"py_filename\" style=\"width:300px\"/>\
			<select id=\"format\">\
				<option value=\"tcx\">.tcx</option>\
				<option value=\"gpx\">.gpx</option>\
			</select>\
			<tt><span id=\"exampleName\">test/asd.tcx</span></tt><br/>\
			(you can include folders, try <tt>/&lt;YYYY&gt;/&lt;MMM&gt;/&lt;NAME&gt;</tt>)<br/>\
		<input type=\"checkbox\" id=\"syncAll\"><label for=\"syncAll\" style=\"display:inline-block\">Sync untagged activities</label></input><br/>\
		<button id=\"OK\">Save</button><button id=\"cancel\" class=\"cancel\">Cancel</button></form>").addClass("dropboxConfig");

	if (tapiriik.ServiceInfo.dropbox.Config.UploadUntagged) $("#syncAll", configPanel).attr("checked","");
	$("#format", configPanel).val(tapiriik.ServiceInfo.dropbox.Config.Format);
	$("#filename", configPanel).val(tapiriik.ConvertDropboxFilenameToDisplay(tapiriik.ServiceInfo.dropbox.Config.Filename));
	tapiriik.UpdateDropboxFilenamePreview(configPanel);

	$("#filename", configPanel).bind("keyup", function(){
		tapiriik.UpdateDropboxFilenamePreview(configPanel);
	});

	$("#format", configPanel).bind("change", function(){
		tapiriik.UpdateDropboxFilenamePreview(configPanel);
	});

	$("#OK", configPanel).click(tapiriik.SaveDropboxConfig);
	$("#cancel", configPanel).click(tapiriik.DismissServiceDialog);
	if (!tapiriik.ServiceInfo.dropbox.Configured) $("#cancel", configPanel).hide();
	tapiriik.CreateServiceDialog("dropbox", configPanel);
	tapiriik.DropboxLastDepth = 1;
	if (tapiriik.ServiceInfo.dropbox.AccessLevel == "full"){
		tapiriik.PopulateDropboxBrowser();
		$("#reauth_up", configPanel).remove();
	} else {
		$("#reauth_down", configPanel).remove();
		$("label", configPanel).first().remove(); //meh
		$("#syncLocationPreamble", configPanel).html("<label>Activities sync to:</label>"); // more meh
		$("#folderList", configPanel).remove();
		var fstack = $("#folderStack", configPanel);
		$("<a class=\"folder inactive\"/>").text("/").appendTo(fstack);
		$("<a class=\"folder inactive\"/>").text("Apps").appendTo(fstack);
		$("<a class=\"folder inactive\"/>").text("tapiriik").appendTo(fstack);
	}
};

tapiriik.ConvertDropboxFilenameToDisplay = function(input){
	// Meh.
	var py_map = {
		"<YYYY>": "%Y",
		"<YY>": "%y",
		"<MMMM>": "%B",
		"<MMM>": "%b",
		"<MM>": "%m",
		"<DD>": "%d",
		"<HH>": "%H",
		"<MIN>": "%M",
		"<NAME>": "#NAME",
		"<TYPE>": "#TYPE"
	};
	for (var key in py_map){
		input = input.replace(new RegExp(py_map[key], "g"), key);
	}
	return input;
};

tapiriik.UpdateDropboxFilenamePreview = function(panel){
	function pad(n, width, z) {
		z = z || '0';
		n = n + '';
		return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
	}
	// %YYYY / %YY
	// %M / %MM / %MMM / %MMMM
	// %DD
	// %HH
	// %MIN
	// %NAME
	// %TYPE
	var now = new Date();
	var months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
	var map = {
		"<YYYY>": now.getFullYear(),
		"<YY>": ("" + now.getFullYear()).substring(2),
		"<MMMM>": months[now.getMonth()],
		"<MMM>": months[now.getMonth()].substring(0,3), // I think
		"<MM>": pad("" + (now.getMonth() + 1),2),
		"<DD>": pad("" + now.getDate(),2),
		"<HH>": pad("" + now.getHours(),2),
		"<MIN>": pad("" + now.getMinutes(),2),
		"<NAME>": "Jumpingpound Ridge",
		"<TYPE>": "Cycling"
	};

	var label_map = {
		"<YYYY>": "year",
		"<YY>": "year",
		"<MMMM>": "month",
		"<MMM>": "month",
		"<MM>": "month",
		"<DD>": "day",
		"<HH>": "hour",
		"<MIN>": "minute",
	};

	var py_map = {
		"<YYYY>": "%Y",
		"<YY>": "%y",
		"<MMMM>": "%B",
		"<MMM>": "%b",
		"<MM>": "%m",
		"<DD>": "%d",
		"<HH>": "%H",
		"<MIN>": "%M",
		"<NAME>": "#NAME", // I'm feeling lazy and don't want to update the backend or migrate existing settings.
		"<TYPE>": "#TYPE",
	};

	var input = $("#filename", panel).val();
	var py_input = input;
	map_keys = [];
	for (var key in map){
		map_keys.push(key);
	}
	map_keys.sort(function(a, b){
		return b.length - a.length;
	});

	for (var key_idx in map_keys){
		key = map_keys[key_idx];
		input = input.replace(new RegExp(key, "ig"), map[key]);
		if (py_map[key]!==undefined){
			py_input = py_input.replace(new RegExp(key, "ig"), py_map[key]);
		}
	}

	input = input + "." + $("#format", panel).val();

	input = input.replace(/([\W_])\1+/g, "$1"); // Doesn't matter for demo data
	input = input.replace(/^([\W_])|([\W_])$/g, "");

	$("#exampleName", panel).text(input);
	$("#py_filename", panel).val(py_input);
};

tapiriik.OpenDropboxInfoDialog = function(){
	var infoPanel = $("<div style=\"max-width:500px\"><h1>You should know...</h1>\
		<p>.GPX files don't include any information about what type of activity the contain, so <b>tapiriik needs your help! Just put what you were doing into the name of the file</b> or place the file into <b>an appropriately named subfolder</b>, e.g. <tt><b>cycling</b>-mar-12-2012.gpx</tt> or <tt><b>run</b>/oldcrow-10k.gpx</tt>. If you want you can <a href=\"/supported-activities\">see the complete list of activities and tags</a>, but don't worry, unrecognized activities will be left alone until you tag them.</p>\
		<button>Sounds good</button></div>");
	$("button", infoPanel).click(function(){
		$.address.value("configure/dropbox/setup");
	});
	tapiriik.CreateServiceDialog("dropbox", infoPanel);
};

tapiriik.SaveDropboxConfig = function(){
	if (tapiriik.DropboxBrowserPath !== undefined && tapiriik.DropboxBrowserPath.length <= 1) {
		return false; // need to select a directory
	}
	if (tapiriik.DropboxSettingsSavePending) return;
	tapiriik.DropboxSettingsSavePending = true;
	$("button#OK").addClass("disabled");
	tapiriik.ServiceInfo.dropbox.Config.SyncRoot = tapiriik.DropboxBrowserPath || tapiriik.ServiceInfo.dropbox.Config.SyncRoot;
	tapiriik.ServiceInfo.dropbox.Config.UploadUntagged = $("#syncAll").is(":checked");
	tapiriik.ServiceInfo.dropbox.Config.Format = $("#format").val();
	tapiriik.ServiceInfo.dropbox.Config.Filename = $("#py_filename").val();
	tapiriik.SaveConfig("dropbox", tapiriik.DismissServiceDialog);
	return false;
};

tapiriik.PopulateDropboxBrowser = function(){
	var cfgPanel = $("form.dropboxConfig");
	var fstack = $("#folderStack", cfgPanel).text("");
	var parts = tapiriik.DropboxBrowserPath.split('/');
	parts.unshift('/');
	var build = "/";
	for (var i = 0; i < parts.length; i++) {
		if (parts[i] == "") continue;
		if (i !== 0) build += parts[i];
		$("<a class=\"folder\"/>").text(parts[i]).attr("path", build).appendTo(fstack).click(tapiriik.DropboxBrowserNavigateDown);
	}

	if (tapiriik.DropboxBrowserPath.length<2) {
		$("button#OK", cfgPanel).addClass("disabled");
	} else {
		$("button#OK", cfgPanel).removeClass("disabled");
	}

	if (tapiriik.DropboxBrowserPath == tapiriik.CurrentDropboxBrowserPath && $("#folderList").children().length) return;

	var depth = tapiriik.DropboxBrowserPath.length; //cheap

	tapiriik.DropboxNavigatingUp = depth <= tapiriik.DropboxLastDepth;

	tapiriik.DropboxLastDepth = depth;

	$("#folderList ul").animate({"margin-left":(tapiriik.DropboxNavigatingUp?1:-1)*$("#folderList").width()});

	if (tapiriik.OutstandingDropboxNavigate !== undefined) tapiriik.OutstandingDropboxNavigate.abort();
	tapiriik.OutstandingDropboxNavigate = $.ajax({
		url: "/dropbox/browse-ajax/",
		data: {path: tapiriik.DropboxBrowserPath},
		success: tapiriik.PopulateDropboxBrowserCallback
	});
	tapiriik.CurrentDropboxBrowserPath = tapiriik.DropboxBrowserPath;
};

tapiriik.PopulateDropboxBrowserCallback = function(data){
	tapiriik.OutstandingDropboxNavigate = undefined;
	$("#folderList").text("");

	list = $("<ul>").appendTo($("#folderList")).css({"margin-left":(tapiriik.DropboxNavigatingUp?-1:1)*$("#folderList").width()});

	if (data.length === 0) {
		$("<h2>no subfolders</h2>").appendTo(list);
	}

	for (var i = 0; i < data.length; i++) {
		var li = $("<li>").appendTo(list);
		$("<a>").text(data[i].replace(tapiriik.DropboxBrowserPath,"").replace(/^\//,"")).attr("path",data[i]).appendTo(li).click(tapiriik.DropboxBrowserNavigateDown);
	}

	$("#folderList ul").animate({"margin-left":0});
};

tapiriik.DropboxBrowserNavigateDown = function(){
	$.address.path("/configure/dropbox/setup" + $(this).attr("path"));
};

tapiriik.PaymentReclaimDialogLinkClicked = function(){
	$.address.value("/payments/claim");
	return false;
};

tapiriik.PaymentPromoDialogLinkClicked = function(){
	$.address.value("/payments/promo");
	return false;
};

tapiriik.OpenPaymentReclaimDialog = function(){
	var form = $("<form><center><div class=\"error\">Unknown email address</div><label for=\"email\" style=\"margin-bottom:7px\">Your PayPal email address</label><input type=\"text\" autofocus style=\"width:300px;text-align:center;\" placeholder=\"remycarrier@gmail.com\" id=\"email\"><br/><button type=\"submit\" id=\"claim\">Claim</button><p>Your payment will be reassociated with the accounts you<br/>are currently connected to, and any you connect in the future.</p></center></form>");
	var pending = false;
	form.bind("submit", function(){
		if (pending) return false;
		pending = true;
		$("button",form).addClass("disabled");
		$.ajax({url:"/payments/claim-ajax",
				type:"POST",
				data:{email: $("#email",form).val()},
				success: function(){
					tapiriik.OpenPaymentReclaimInitiatedDialog($("#email",form).val());
				},
				error: function(data){
					$(".error",form).show();
				$("button",form).removeClass("disabled");
				pending = false;
				}});
		return false;
	});
	tapiriik.CreateServiceDialog("tapiriik",form);
};

tapiriik.OpenPaymentReclaimInitiatedDialog = function(email){
	var form = $("<center><h1>The email is on its way</h1>An email has been sent to <b><span class=\"email\"></span></b> with a link to reclaim your payment.<br/>Don't have access to <b><span class=\"email\"></span></b> any more? <a href=\"mailto:contact@tapiriik.com\">Get in touch</a><br/><button id=\"acknowledge\">I'll be waiting</button></center>");
	$(".email", form).text(email);
	$("#acknowledge", form).click(function(){
		$.address.value("");
	});
	setInterval(function(){
		$.ajax({"url":"/payments/claim-wait-ajax", success:function(data){
			if (!data.claimed) return;
			$.address.value("");
			window.location.reload();
		}});
	}, 1000);
	tapiriik.CreateServiceDialog("tapiriik",form);
};

tapiriik.OpenPaymentReclaimCompletedDialog = function(){
	var form = $("<center><h1>You're good to go!</h1>Your payment has been reclaimed &amp; associated with the services you are currently connected to, and any you connect in the future.<br/><button id=\"acknowledge\">Great</button></center>");
	$("#acknowledge", form).click(function(){
		$.address.value("");
	});
	tapiriik.CreateServiceDialog("tapiriik",form);
};

tapiriik.OpenPaymentPromoDialog = function(){
	var form = $("<form><center><div class=\"error\">Invalid promo code</div><label for=\"code\" style=\"margin-bottom:7px\">Your promo code</label><input type=\"text\" autofocus style=\"width:300px;text-align:center;\" placeholder=\"PTARMIGANS-4-LIFE\" id=\"code\"><br/><button type=\"submit\" id=\"claim\">Claim</button><p>This promo code will be associated with the accounts you are <br/> currently connected to. It <b>can</b> be transferred between accounts.</p></center></form>");
	var pending = false;
	form.bind("submit", function(){
		if (pending) return false;
		pending = true;
		$("button", form).addClass("disabled");
		$.ajax({url:"/payments/promo-claim-ajax",
				type:"POST",
				data:{code: $("#code",form).val()},
				success: function(){
					tapiriik.OpenPaymentPromoClaimCompletedDialog();
				},
				error: function(data){
					$(".error",form).show();
				$("button",form).removeClass("disabled");
				pending = false;
				}});
		return false;
	});
	tapiriik.CreateServiceDialog("tapiriik", form);
};

tapiriik.OpenPaymentPromoClaimCompletedDialog = function(){
	var form = $("<center><h1>You're good to go!</h1>Your account has been set up for automatic synchronization! You can always transfer the promo code to another account at any time.<br/><button id=\"acknowledge\">Great</button></center>");
	$("#acknowledge", form).click(function(){
		$.address.value("");
		window.location.reload();
	});
	tapiriik.CreateServiceDialog("tapiriik",form);
};

tapiriik.OpenSyncSettingsDialog = function(){
	$(".syncSettingsBlock").slideDown(250);

	// Apply datepicker here,
	// because it is associated with an HTML element.
	tapiriik.ApplyDatepicker();
};

tapiriik.CloseSyncSettingsDialog = function(){
	$(".syncSettingsBlock").slideUp(250);
};

tapiriik.CreateConfigPanel = function(serviceID, contents){
	var configTray = $("<div>").addClass("config").append($("<div>").addClass("arrow")).append(contents).appendTo($(".service#"+serviceID));

	$(configTray).hide();
	$(".service#"+serviceID+" .controls").slideUp(100, function(){
		$(configTray).slideDown(200);
	});
};

tapiriik.DoDismissConfigPanel = function(){
	$(".config").slideUp(200, function(){
		$(".controls", $(this).parent()).slideDown(200);
		$(this).remove();
	});
};

tapiriik.PageOpened = new Date();

tapiriik.CreateServiceDialog = function(serviceID, contents) {
	var animationMultiplier = 1;
	if ((new Date()) - tapiriik.PageOpened < 1000){
		animationMultiplier = 0;
	}
	if ($(".dialogWrap").size()>0){
		$(".dialogWrap").fadeOut(100 * animationMultiplier, function(){
			$(".dialogWrap").remove();
			tapiriik.CreateServiceDialog(serviceID, contents);
		});
		return;
	}

	var icon;
	if (serviceID != "tapiriik"){
		icon = $("<img>").attr("src", tapiriik.StaticURL + "img/services/" + serviceID + "_l.png"); // Magic URL :\
	} else {
		icon = $("<div>").addClass("logo inline").text("tapiriik");
	}
	popover = $("<div>").addClass("dialogPopoverWrap").append(tapiriik.CreatePopover(contents).css({"position":"relative"}));
	popover.css({"position":"relative", "width":"100%"});
	var dialogWrap = $("<div>").addClass("dialogWrap").append(icon).append(popover).hide();
	$(".contentWrap").append(dialogWrap);
	$(".mainBlock").fadeOut(250 * animationMultiplier, function(){
		$(dialogWrap).fadeIn(250 * animationMultiplier);
	});
};

tapiriik.DismissServiceDialog = function(e){
	if (e) e.preventDefault();
	$.address.value("");
	return false;
};

tapiriik.DoDismissServiceDialog = function(){
	$(".dialogWrap").fadeOut(250, function(){
		$(".dialogWrap").remove();
		$(".mainBlock").fadeIn(250);
	});
};
// I started writing a popover function, then decided it sucked, so I did what you see above
tapiriik.CreatePopover = function(contents){
	var popoverStruct = $("<div class=\"popover\"><div class=\"popoverOuterBorder\"><div class=\"popoverArrow\"><div class=\"popoverArrowInner\"></div></div><div class=\"popoverInner\"></div></div></div>");
	$(".popoverInner", popoverStruct).append(contents);
	return popoverStruct;
};

tapiriik.ClearExceptionLinkClicked = function(){
	var service = $(this).attr("service");
	var group = $(this).attr("group");

	var animationComplete = false;
	var postComplete = false;
	$.post("/sync/errors/" + service + "/clear/" + group, function(){
		postComplete = true;
		if (animationComplete && postComplete) window.location.reload(); // Imagine AngularJS right here.
	});

	$(this).closest(".userException").slideUp(function(){
		animationComplete = true;
		if (animationComplete && postComplete) window.location.reload();
	});
	return false;
};

tapiriik.ImmediateSyncRequested = function(){
	if (!$(".syncButton").hasClass("active")) return false;

	$.post("/sync/schedule/now");
	tapiriik.NextSync = new Date();
	tapiriik.LastSync = new Date();

	tapiriik.RefreshSyncCountdown();
	return false;
};

tapiriik.PendingSyncStatusUpdate = false;

tapiriik.UpdateSyncCountdown = function(){
	if (tapiriik.PendingSyncStatusUpdate) return;
	tapiriik.PendingSyncStatusUpdate = true;
	$.ajax({"url":"/sync/status", success:function(data){
		tapiriik.PendingSyncStatusUpdate = false;
		$rootScope.$apply(function(){ // Tie us into Angularland
		tapiriik.NextSync = data.NextSync !== null ? new Date(data.NextSync) : null;
		tapiriik.LastSync = data.LastSync !== null ? new Date(data.LastSync) : null;
		if (tapiriik.SyncHash !== undefined && tapiriik.SyncHash != data.Hash){
			window.location.reload(); // show them the whatever's new
		}
		tapiriik.SyncHash = data.Hash;
		tapiriik.SyncErrors = data.Errors;
		tapiriik.Synchronizing = data.Synchronizing;
		tapiriik.SynchronizationProgress = data.SynchronizationProgress;
		tapiriik.SynchronizationStep = data.SynchronizationStep;
		tapiriik.SynchronizationWaitTime = data.SynchronizationWaitTime;
		});
		tapiriik.RefreshSyncCountdown();
	}, error:function(req, opts, error){
		tapiriik.PendingSyncStatusUpdate = false;
		// I trashed the session store somehow, and everyone got logged out.
		if (req.status == 403) {
			window.location.reload();
		}
	}});
};
tapiriik.FormatTimespan = function(spanMillis){
	if (Math.abs(spanMillis/1000) > 60 * 60){
		return Math.round(spanMillis/1000/60/60)+" hour"+(Math.round(spanMillis/1000/60/60)!=1?"s":"");
	}
	else if (Math.abs(spanMillis/1000) > 60){
		return Math.round(spanMillis/1000/60)+" minute"+(Math.round(spanMillis/1000/60)!=1?"s":"");
	} else {
		return Math.ceil(spanMillis/1000)+" second"+(Math.ceil(spanMillis/1000)!=1?"s":"");
	}
};
tapiriik.RefreshSyncCountdown = function(){
	var sync_button_active = false;
	var sync_button_engaged = false;
	var sync_button_queuing = false;
	var sync_state_text = "";
	var sync_post_text = "";
	if (tapiriik.SyncHash !== undefined){
		var delta = tapiriik.NextSync - (new Date());
		if (delta>0 || (tapiriik.NextSync === null && !tapiriik.Synchronizing)){
			$("#syncButton").show();
			var inCooldown = ((new Date()) - tapiriik.LastSync) <= tapiriik.MinimumSyncInterval*1000;
			sync_button_active = !inCooldown;
			if (tapiriik.NextSync !== null){
				sync_state_text = "Next sync in " + tapiriik.FormatTimespan(delta);
			} else {
				if (inCooldown){
					sync_state_text = "Synchronized";
				}
			}
			if (tapiriik.FastUpdateCountdownTimer !== undefined){
				clearInterval(tapiriik.FastUpdateCountdownTimer);
				tapiriik.FastUpdateCountdownTimer = undefined;
			}
		} else {
			sync_button_active = false;
			if (!tapiriik.Synchronizing){
				var waitTimeMessage = "";
				if (tapiriik.SynchronizationWaitTime > 60) { // Otherwise you'd expect a countdown, which this is generally not.
					waitTimeMessage = " (approx. " + tapiriik.FormatTimespan(tapiriik.SynchronizationWaitTime * 1000) + ")";
				}
				sync_state_text = "Queuing" + waitTimeMessage;
				sync_button_queuing = true;
			} else {
				sync_button_engaged = true;
				var progress = "";
				if (tapiriik.SynchronizationStep == "list") {
					sync_state_text = "Checking " + tapiriik.ServiceInfo[tapiriik.SynchronizationProgress].DisplayName;
				} else {
					sync_state_text = Math.round(tapiriik.SynchronizationProgress*100) + "% complete";
				}
			}
			if (tapiriik.FastUpdateCountdownTimer === undefined){
				tapiriik.FastUpdateCountdownTimer = setInterval(tapiriik.UpdateSyncCountdown, 6000);
			}

		}

		var measureText = function(txt){
			var temp = $(".syncButtonAttachment:first").clone();
			temp.text(txt);
			$("body").append(temp);
			temp.css("width", "auto");
			var width = temp.width();
			temp.remove();
			return width;
		};
		$(".syncButton").toggleClass("queuing", sync_button_queuing);
		$(".syncButton").toggleClass("engaged", sync_button_engaged);
		$(".syncButton").toggleClass("active", sync_button_active).attr("title", sync_button_active ? "Synchronize now" : (sync_button_engaged ? "Synchronizing now..." : "You just synchronized!"));
		// I don't like this, so I'm only doing it for the left-hand stuff. Note to future self several years down the road: still in use, I know, right?
		if (sync_state_text != $(".syncButtonAttachment.left").text()){
			var currentWidth = $(".syncButtonAttachment.left").width();
			var newWidth = measureText(sync_state_text);
			if (currentWidth >= newWidth) {
				$(".syncButtonAttachment.left").text(sync_state_text);
			}
			$(".syncButtonAttachment.left").animate({"width": newWidth + "px"}, 150, function(){
				if (currentWidth < newWidth) {
					$(".syncButtonAttachment.left").text(sync_state_text);
				}
			});
		}
		$(".syncButtonAttachment.right").text(sync_post_text);
		if (sync_state_text) {
			$(".syncButtonAttachment.left").show(200);
		} else {
			$(".syncButtonAttachment.left").hide(200);
		}
		if (sync_post_text) {
			$(".syncButtonAttachment.right").show(200);
		} else {
			$(".syncButtonAttachment.right").hide(200);
		}
		setTimeout(function(){$(".syncButtonBlock").animate({"opacity":1});}, 500);
	}
};

tapiriik.ShowDebugInfo = function(){
	if ($(".debugInfo").length>0 || window.location.pathname != "/") return;
	var infoPane = $("<div class=\"debugInfo\"><h3>Diagnostics</h3></div>");
	if (tapiriik.User !== undefined) {
		infoPane.append($("<div><b>User ID:</b> <tt>" + tapiriik.User.ID + "</tt></div>"));
		infoPane.append($("<div><b>User TZ:</b> <tt>" + tapiriik.User.Timezone + "</tt></div>"));
	}
	infoPane.append($("<div><b>System:</b> <tt>" + tapiriik.SiteVer + "</tt></div>"));
	infoPane.hide();
	$(".content").append(infoPane);
	infoPane.slideDown();
};

/*
var logo_variant;
tapiriik.CycleLogo = function(){
	var variants = ["arabic", "hebrew", "hindi", "inuktitut", "punjabi"];
	if (logo_variant) {
		variants.splice(variants.indexOf(logo_variant), 1);
	}
	logo_variant = variants[Math.floor(Math.random() * variants.length)];
	var img = $("<img>").attr("src", tapiriik.StaticURL + "img/tapiriik-" + logo_variant + ".png");
	$(".logo a").fadeOut(function(){
		$(this).remove();
		img.appendTo($("<a>").attr("href", "/").prependTo($(".logo")).hide().fadeIn());
	});
};*/

tapiriik.Logout = function(){
	$().redirect("/auth/logout", {csrfmiddlewaretoken: csrftoken});
};

tapiriik.AB_Begin = function(key){
	$.post("/ab/begin/" + key);
};

tapiriik.LoadStyle = function(url) {
	var first, head, link;

	link = document.createElement("link");
	link.href = url;
	link.rel = "stylesheet";
	link.type = "text/css";
	head = document.head;
	first = head.firstChild;

	// Prepend styles in 'head' section
	head.insertBefore(link, first);
};

tapiriik.LoadScript = function(url, callback, error) {
	var script = document.createElement("script");

	// If "fallback" url passed as second argument
	var args = Array.prototype.slice.call(arguments);
	if (typeof callback === "string") {
		var fallbackUrl = callback;
		callback = error;
		var errorOri = args[3];
		error = function() {
			tapiriik.LoadScript(fallbackUrl, callback, errorOri);
		};
	}

	var cb = function() {
		if (typeof callback === "function") {
			callback();
		}
	};

	// IE
	if (script.readyState) {
		script.onreadystatechange = function() {
			if (script.readyState === "loaded" || script.readyState === "complete") {
				script.onreadystatechange = null;
				cb();
			}
		};

	// Normal browsers
	} else {
		if (typeof error === "function") {
			script.onerror = error;
		}
		script.onload = cb;
	}

	script.src = url;
	script.async = true;
	(document.body || document.head).appendChild(script);
};

tapiriik.ApplyDatepicker = function() {
	var element = document.querySelector(".js-datepicker");
	if (!element || this.ApplyDatepicker._loaded === true) {
		return;
	}
	this.ApplyDatepicker._loaded = true;

	var doneFunc = function() {
		if (typeof Pikaday === "undefined") {
			return;
		}

		function dateFormat(date) {
			var strArray = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
			var d = date.getDate();
			var m = strArray[date.getMonth()];
			var y = date.getFullYear();
			return [d, m, y].join(" ");
		}

		var picker = new Pikaday({
			field: element,
			firstDay: 1,
			toString: dateFormat
		});
	};

	var errorFunc = function() {
		tapiriik.LoadStyle("/static/js/datepicker/pikaday-1.6.1.min.css");
		tapiriik.LoadScript("/static/js/datepicker/pikaday-1.6.1.min.js", doneFunc);
	};

	tapiriik.LoadStyle("//cdnjs.cloudflare.com/ajax/libs/pikaday/1.6.1/css/pikaday.min.css");
	tapiriik.LoadScript("//cdnjs.cloudflare.com/ajax/libs/pikaday/1.6.1/pikaday.min.js", doneFunc, errorFunc);
};

$(window).load(tapiriik.Init);

// Seems like a waste of a HTTP request just for this...
(function(d){d.fn.redirect=function(a,b,c){void 0!==c?(c=c.toUpperCase(),"GET"!=c&&(c="POST")):c="POST";if(void 0===b||!1==b)b=d().parse_url(a),a=b.url,b=b.params;var e=d("<form></form");e.attr("method",c);e.attr("action",a);for(var f in b)a=d("<input />"),a.attr("type","hidden"),a.attr("name",f),a.attr("value",b[f]),a.appendTo(e);d("body").append(e);e.submit()};d.fn.parse_url=function(a){if(-1==a.indexOf("?"))return{url:a,params:{}};var b=a.split("?"),a=b[0],c={},b=b[1].split("&"),e={},d;for(d in b){var g= b[d].split("=");e[g[0]]=g[1]}c.url=a;c.params=e;return c}})(jQuery);