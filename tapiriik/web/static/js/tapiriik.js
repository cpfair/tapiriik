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


tapiriik = {};

tapiriik.Init = function(){
	// ...
	//tapiriik.CreateServiceDialog("strava",$("<span>asddsa<br/>asddsa<br/>asddsa<br/>asddsa<br/>asddsa<br/></span>"));
	$(".service a.authDialog").click(tapiriik.AuthDialogLinkClicked);
	$.address.change(tapiriik.AddressChanged);
	

};

tapiriik.AddressChanged=function(){
	var components = $.address.pathNames();
	if (components[0]=="auth") {
		tapiriik.OpenAuthDialog(components[1]);
		return;
	}
	tapiriik.DismissServiceDialog();
};

tapiriik.AuthDialogLinkClicked = function(e){
	$.address.value("auth/"+this.parentNode.id);
	return false;
};

tapiriik.IFrameOAuthReturn=function(){
	$.address.value("/");
	window.location.reload();
};

tapiriik.OpenAuthDialog = function(svcId){
	var authLink = $(".service#"+svcId+" a.authDialog");
	if (authLink.length == 0) return;

	var mode = authLink.attr("mode");
	var contents;
	if (mode == "oauth"){
		contents = $("<iframe>").attr("src",authLink.attr("href")).attr("id",svcId);
	} else if (mode == "direct") {
		contents = tapiriik.CreateDirectLoginForm(svcId);
	}
	tapiriik.CreateServiceDialog(svcId, contents);
};

tapiriik.CreateDirectLoginForm = function(svcId){
	var form = $("<form><div class=\"error\">There was a problem logging you in</div><label for=\"email\">Email</label><input autofocus type=\"email\" id=\"email\"/><label for=\"password\">Password</label><input type=\"password\" id=\"password\"><br/><center><button type=\"submit\" >Log in</button></center></form>");
	var loginPending = false;
	form.bind("submit", function(){
		if (loginPending) return false;
		loginPending=true;
		$("button",form).addClass("disabled");
		$.post("/auth/login-ajax/"+svcId,{csrfmiddlewaretoken:csrftoken, username:$("#email",form).val(),password:$("#password",form).val()}, function(data){
			loginPending = false;
			if (data.success) {
				$.address.value("/");
				window.location.reload();
			} else {
				$(".error",form).show();
			}
			$("button",form).removeClass("disabled");
		}, "json");
		return false;
	});
	return form;
};

tapiriik.CreateServiceDialog = function(serviceID, contents) {
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