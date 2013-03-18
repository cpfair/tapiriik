// using jQuery
function getCookie(name) {
    var cookieValue = null;
    if (document.cookie && document.cookie != '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = jQuery.trim(cookies[i]);
            // Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) == (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}
var csrftoken = getCookie('csrftoken');

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

tapiriik = {};
tapiriik.PreviousURLComponents = [];

tapiriik.Init = function(){
	// ...
	$("#syncButton").click(tapiriik.ImmediateSyncRequested);
	$(".service a.authDialog").click(tapiriik.AuthDialogLinkClicked);
	$(".service a.configDialog").click(tapiriik.ConfigDialogLinkClicked);
	$(".service a.deauthDialog").click(tapiriik.DeauthDialogLinkClicked);
	
	if (tapiriik.User !== undefined){
		if (tapiriik.User.ConnectedServicesCount > 1){
			tapiriik.UpdateCountdownTimer = setInterval(tapiriik.UpdateSyncCountdown, 60000);
			tapiriik.RefreshCountdownTimer = setInterval(tapiriik.RefreshSyncCountdown, 1000);
			tapiriik.UpdateSyncCountdown();
		}
	}
	$(".logo").click(function(e){
		if (e.shiftKey){
			tapiriik.ShowDebugInfo();
			return false;
		}
	});

	if (tapiriik.User !== undefined) {
		for (var i in tapiriik.ServiceInfo) {
			if (tapiriik.ServiceInfo[i].Connected && tapiriik.ServiceInfo[i].Configurable && !tapiriik.ServiceInfo[i].Configured){
				tapiriik.OpenConfigDialog(i);
				break; // we can nag them again if there's >1
			}
		}
	}

	$.address.change(tapiriik.AddressChanged);
	tapiriik.AddressChanged();

};
$.address.wrap(true);

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
	} else if (components[0]=="disconnect") {
		tapiriik.OpenDeauthDialog(components[1]);
		return;
	} else if (components[0]=="configure") {
		if (components[1]=="dropbox"){
			if (unchangedDepth<=1) {
				tapiriik.DropboxBrowserPath = tapiriik.ServiceInfo.dropbox.Config.SyncRoot;
				$.address.value("configure/dropbox" + tapiriik.DropboxBrowserPath); // init directory, meh
				tapiriik.OpenDropboxConfigDialog();
			} else {
				tapiriik.DropboxBrowserPath = "/" + components.slice(2).join("/");
				tapiriik.PopulateDropboxBrowser();
			}
			return;
		}
	} else if (components[0] == "dropbox") {
		if (components[1] == "info"){
			tapiriik.OpenDropboxInfoDialog();
			return;
		}
	}
	tapiriik.DoDismissServiceDialog();
};

tapiriik.SaveConfig = function(svcId, config, callback) {
	$.post("/configure/save/"+svcId, {"config": JSON.stringify(tapiriik.ServiceInfo[svcId].Config)},function(){
		$.address.value("");
		window.location.reload();
	});

};

tapiriik.AuthDialogLinkClicked = function(e){
	$.address.value("auth/"+$(this).attr("service"));
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
		if (tapiriik.ServiceInfo[svcId].NoFrame){
			window.location = tapiriik.ServiceInfo[svcId].AuthorizationURL;
			return;
		} else {
			contents = $("<iframe>").attr("src",tapiriik.ServiceInfo[svcId].AuthorizationURL).attr("id",svcId);
		}
	} else if (mode == "direct") {
		contents = tapiriik.CreateDirectLoginForm(svcId);
	}
	tapiriik.CreateServiceDialog(svcId, contents);
};

tapiriik.OpenDeauthDialog = function(svcId){
	var form = $("<form><center><button id=\"disconnect\">Disconnect</button><button id=\"cancel\" class=\"cancel\">Never mind</button></center></form><h2>(nothing will be deleted)</h2>");
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
	var form = $("<form><div class=\"error\">There was a problem logging you in</div><label for=\"email\">Email</label><input autofocus type=\"email\" id=\"email\"/><label for=\"password\">Password</label><input type=\"password\" id=\"password\"><br/><center><button type=\"submit\" >Log in</button></center></form>");
	var loginPending = false;
	form.bind("submit", function(){
		if (loginPending) return false;
		loginPending=true;
		$("button",form).addClass("disabled");
		$.post("/auth/login-ajax/"+svcId,{username:$("#email",form).val(),password:$("#password",form).val()}, function(data){
			
			if (data.success) {
				$.address.value("");
				window.location.reload();
			} else {
				$(".error",form).show();
				$("button",form).removeClass("disabled");
				loginPending = false;
			}
		}, "json");
		return false;
	});
	return form;
};

tapiriik.OpenConfigDialog = function(svcId){
	if (svcId == "dropbox" && !tapiriik.ServiceInfo.dropbox.Configured) {
		$.address.value("dropbox/info");
		return;
	}
	$.address.value("configure/" + svcId);
};
tapiriik.OpenDropboxConfigDialog = function(){
	var configPanel = $("<form class=\"dropboxConfig\"><h1>Configure Dropbox Sync</h1><label>Select sync folder</label><div id=\"folderList\"></div><div id=\"folderStackOuter\">Will sync to <span id=\"folderStack\"></span></div><input type=\"checkbox\" id=\"syncAll\"><label for=\"syncAll\" style=\"display:inline-block\">Sync untagged activities</label></input><br/><button id=\"OK\">Save</button><button id=\"cancel\" class=\"cancel\">Cancel</button><button id=\"disconnect\" class=\"delete\">Disconnect</button></form>").addClass("dropboxConfig");

	if (tapiriik.ServiceInfo.dropbox.Config.UploadUntagged) $("#syncAll", configPanel).attr("checked","");
	$("#OK", configPanel).click(tapiriik.SaveDropboxConfig);
	$("#cancel", configPanel).click(tapiriik.DismissServiceDialog);
	if (!tapiriik.ServiceInfo.dropbox.Configured) $("#cancel", configPanel).hide();
	$("#disconnect", configPanel).click(function(){
		$.address.value("disconnect/dropbox");
		return false;
	});
	tapiriik.CreateServiceDialog("dropbox", configPanel);
	tapiriik.DropboxLastDepth = 1;
	tapiriik.PopulateDropboxBrowser();
};

tapiriik.OpenDropboxInfoDialog = function(){
	var infoPanel = $("<div style=\"max-width:500px\"><h1>You should know...</h1>\
		<p>.GPX files don't include any information about what type of activity the contain, so <b>tapiriik needs your help! Just put what you were doing into the name of the file</b> or place the file into <b>an appropriately named subfolder</b>, e.g. <tt><b>cycling</b>-mar-12-2012.gpx</tt> or <tt><b>run</b>/oldcrow-10k.gpx</tt>. If you want you can <a href=\"/supported-activities\">see the complete list of activities and tags</a>, but don't worry, unrecognized activities will be left alone until you tag them.</p>\
		<button>Sounds good</button></div>");
	$("button", infoPanel).click(function(){
		$.address.value("configure/dropbox");
	});
	tapiriik.CreateServiceDialog("dropbox", infoPanel);
};

tapiriik.SaveDropboxConfig = function(){
	if (tapiriik.DropboxBrowserPath.length <= 1) {
		return false; // need to select a directory
	}
	tapiriik.ServiceInfo.dropbox.Config.SyncRoot = tapiriik.DropboxBrowserPath;
	tapiriik.ServiceInfo.dropbox.Config.UploadUntagged = $("#syncAll").is(":checked");
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
	tapiriik.OutstandingDropboxNavigate = $.ajax("/dropbox/browse-ajax/" + tapiriik.DropboxBrowserPath).success(tapiriik.PopulateDropboxBrowserCallback);
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
	$.address.path("/configure/dropbox" + $(this).attr("path"));
};

tapiriik.CreateServiceDialog = function(serviceID, contents) {
	$(".dialogWrap").remove();
	var origIcon = $(".service#"+serviceID+" .icon img");
	var icon = origIcon.clone().attr("src", origIcon.attr("lgsrc")).hide();
	popover = $("<div>").addClass("dialogPopoverWrap").append(tapiriik.CreatePopover(contents).css({"position":"relative"}));
	popover.css({"position":"relative","display":"none", "width":"100%"});
	var dialogWrap = $("<div>").addClass("dialogWrap").append(icon).append(popover);
	$(".contentWrap").append(dialogWrap);
	$(".mainBlock").fadeOut(250, function(){
		popover.fadeIn(250);
		icon.fadeIn(250);
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

tapiriik.ImmediateSyncRequested = function(){
	if (!$("#syncButton").hasClass("active")) return false;

	$.get("/sync/schedule/now");
	tapiriik.NextSync = new Date();
	tapiriik.LastSync = new Date();

	tapiriik.RefreshSyncCountdown();
	return false;
};

tapiriik.UpdateSyncCountdown = function(){
	$.ajax({"url":"/sync/status", success:function(data){
		tapiriik.NextSync = new Date(data.NextSync);
		tapiriik.LastSync = new Date(data.LastSync);
		if (tapiriik.SyncErrors !== undefined && tapiriik.SyncErrors.toString() != data.Errors.toString()){
			window.location.reload(); // show them the errors
		}
		tapiriik.SyncErrors = data.Errors;
		tapiriik.Synchronizing = data.Synchronizing;
		tapiriik.RefreshSyncCountdown();
	}});
};
tapiriik.FormatTimespan = function(spanMillis){
	if (Math.abs(spanMillis/1000)>60){
		return Math.round(spanMillis/1000/60)+" minute"+(Math.ceil(spanMillis/1000/60)!=1?"s":"");
	} else {
		return Math.ceil(spanMillis/1000)+" second"+(Math.ceil(spanMillis/1000)!=1?"s":"");
	}
};
tapiriik.RefreshSyncCountdown = function(){
	if (tapiriik.NextSync !== undefined){
		var delta = tapiriik.NextSync - (new Date());
		if (delta>0){
			$("#syncButton").show();
			$("#syncButton").text(tapiriik.FormatTimespan(delta));
			if (((new Date()) - tapiriik.LastSync) > tapiriik.MinimumSyncInterval*1000) {
				$("#syncButton").addClass("active");
			} else {
				$("#syncButton").removeClass("active");
			}
			$("#syncStatusPreamble").text("Next synchronization in ");
			if (tapiriik.FastUpdateCountdownTimer !== undefined){
				clearInterval(tapiriik.FastUpdateCountdownTimer);
				tapiriik.FastUpdateCountdownTimer = undefined;
			}
		} else {
			$("#syncButton").hide();

			if (!tapiriik.Synchronizing){
				$("#syncStatusPreamble").text("Queuing to synchronize");
			} else {
				$("#syncStatusPreamble").text("Synchronizing now");
			}
			
			if (tapiriik.FastUpdateCountdownTimer === undefined){
				tapiriik.FastUpdateCountdownTimer = setInterval(tapiriik.UpdateSyncCountdown, 1000);
			}
		}
		$(".syncStatus").show();
	}
};

tapiriik.ShowDebugInfo = function(){
	if ($(".debugInfo").length>0 || window.location.pathname != "/") return;
	var infoPane = $("<div class=\"debugInfo\"><h3>Diagnostics</h3></div>");
	if (tapiriik.User !== undefined) infoPane.append($("<div><b>User ID:</b> <tt>" + tapiriik.User.ID + "</tt></div>"));
	infoPane.append($("<div><b>System:</b> <tt>" + tapiriik.SiteVer + "</tt></div>"));
	infoPane.hide();
	$(".content").append(infoPane);
	infoPane.slideDown();
};

$(window).load(tapiriik.Init);