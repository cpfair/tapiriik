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

tapiriik.Init = function(){
	// ...
	//tapiriik.CreateServiceDialog("strava",$("<span>asddsa<br/>asddsa<br/>asddsa<br/>asddsa<br/>asddsa<br/></span>"));
	$(".service a.authDialog").click(tapiriik.AuthDialogLinkClicked);
	$(".service a.deauthDialog").click(tapiriik.DeauthDialogLinkClicked);
	$.address.change(tapiriik.AddressChanged);
	tapiriik.AddressChanged();
};

tapiriik.AddressChanged=function(){
	var components = $.address.pathNames();
	if (components[0]=="auth") {
		tapiriik.OpenAuthDialog(components[1]);
		return;
	} else if (components[0]=="disconnect") {
		tapiriik.OpenDeauthDialog(components[1]);
		return;
	}
	tapiriik.DismissServiceDialog();
};

tapiriik.AuthDialogLinkClicked = function(e){
	$.address.value("auth/"+$(this).attr("service"));
	return false;
};

tapiriik.DeauthDialogLinkClicked = function(e){
	$.address.value("disconnect/"+$(this).attr("service"));
	return false;
};

tapiriik.IFrameOAuthReturn=function(){
	$.address.value("/");
	window.location.reload();
};

tapiriik.OpenAuthDialog = function(svcId){
	var mode = tapiriik.ServiceInfo[svcId].AuthenticationType;
	var contents;
	if (mode == "oauth"){
		contents = $("<iframe>").attr("src",tapiriik.ServiceInfo[svcId].AuthorizationURL).attr("id",svcId);
	} else if (mode == "direct") {
		contents = tapiriik.CreateDirectLoginForm(svcId);
	}
	tapiriik.CreateServiceDialog(svcId, contents);
};

tapiriik.OpenDeauthDialog = function(svcId){
	var form = $("<form><center><button id=\"disconnect\">Disconnect</button><button id=\"cancel\">Nevermind</button></center></form><h2>(nothing will be deleted)</h2>");
	form.bind("submit", function() {return false;});
	$("#disconnect", form).click(function(){
		$.post("/auth/disconnect-ajax/"+svcId, {success: function(data){
			$.address.value("/");
			window.location.reload();
		}});
	});
	$("#cancel", form).click(function(){
		$.address.value("/");
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
				$.address.value("/");
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
tapiriik.DismissServiceDialog = function(){
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


$(window).load(tapiriik.Init);