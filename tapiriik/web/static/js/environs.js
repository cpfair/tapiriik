tapiriik.environs = {};
tapiriik.environs.tod = 0.30; // 0.5 = noon, 0 = midnight

tapiriik.environs.sunrise = 0.32; //can't overlap, I know, unrealistic
tapiriik.environs.sunset = 0.81;
tapiriik.environs.moonrise = 0.85;
tapiriik.environs.moonset = 0.25;

tapiriik.environs.daytime = tapiriik.environs.sunset - tapiriik.environs.sunrise;
tapiriik.environs.nighttime = tapiriik.environs.moonset + 1 - tapiriik.environs.moonrise;
tapiriik.environs.moonPhase = 0.5; 
tapiriik.environs.mountains = [
								{offset:70, width:600, height:225, color:[149,203,229]},
								{offset:-200, width:600, height:500, color:[142,193,218]},
								{offset:0, width:400, height:600, color:[166,207,227]}
								];
tapiriik.environs.days = 0;
tapiriik.environs.starfieldAge = 0;

function debugWrite(ctx, text, y){
	//return;
	ctx.save();
	ctx.globalAlpha = 1;
	ctx.font = "20px Arial";
	ctx.fillStyle="white";
	ctx.fillText(text,10,y-25);
	ctx.restore();
}


tapiriik.environs.Init = function(){
	tapiriik.environs.CalculateMoonPhase();
	tapiriik.environs.canvas = $("<canvas>").appendTo($(".environsWrap"));
	tapiriik.environs.canvas.append($("div.environs"));
	tapiriik.environs.starCanvas = document.createElement('canvas');
	tapiriik.environs.fgCanvas = document.createElement('canvas');
	tapiriik.environs.Resize();
	//$(".contentOuter").hide();
	
	setInterval(tapiriik.environs.Draw, 30000);
};

var renderToCanvas = function (width, height, renderFunction) {
    var buffer = document.createElement('canvas');
    buffer.width = width;
    buffer.height = height;
    renderFunction(buffer.getContext('2d'));
    return buffer;
};

tapiriik.environs.CalculateMoonPhase = function(){
	var anchors = [18, 0, 11, 22, 3, 14, 25, 6, 17, 28, 9, 20, 1, 12, 23, 4, 15, 26, 7];
	var offsets = [-1, 1, 0, 1, 2, 3, 4, 5, 7, 9, 9];
	var now = new Date()
	var age = (anchors[(now.getFullYear() + 1) % 19] + ((now.getDate() + offsets[now.getMonth()]) % 30));
	tapiriik.environs.moonPhase = age / 15 / 2;
	if (tapiriik.environs.moonPhase < 0) tapiriik.environs.moonPhase += 1;
};

function arrToRgba(arr){
	if (arr.length == 3){
		return "rgba("+arr[0]+","+arr[1]+","+arr[2]+",1)";
	} else {
		return "rgba("+arr[0]+","+arr[1]+","+arr[2]+","+arr[3]+")";
	}
}
function arrMix(a, b, balance){
	var c=[];
	for (var i = 0; i < a.length; i++) {
		c[i]=Math.round(a[i]*(1-balance) + b[i]*balance);
	}
	return c;
}
tapiriik.environs.GenerateStarfield = function(){
	var ctx = $(tapiriik.environs.starCanvas).get(0).getContext("2d");
	var width = $(tapiriik.environs.starCanvas).attr("width") ;
	var height = $(tapiriik.environs.starCanvas).attr("height");
	ctx.clearRect ( 0 , 0 , width, height );

	ctx.fillStyle = "white";
	var starCt = Math.random()*1500;
	for (var i = 0; i < starCt; i++) {
		var starX = Math.random()*height
		ctx.globalAlpha = Math.random()/3+0.66;
		ctx.beginPath();
		ctx.arc(Math.random()*width, starX, Math.random()*2, 0, Math.PI*2, false);
		ctx.fill();
	}
	
	ctx.globalAlpha = 1;
	ctx.beginPath();
	ctx.arc(width/2 , height/2, 3, 0, Math.PI*2, false);
	ctx.fill();

	ctx.fillStyle = "white";
	ctx.beginPath();
	ctx.arc(width/2 + 150 , height/2, 2, 0, Math.PI*2, false);
	ctx.fill();
	ctx.beginPath();
	ctx.arc(width/2 + 205 , height/2 + 17, 3, 0, Math.PI*2, false);
	ctx.fill();
	ctx.beginPath();
	ctx.arc(width/2 + 150 , height/2 + 120, 1, 0, Math.PI*2, false);
	ctx.fill();

	ctx.beginPath();
	ctx.arc(width/2 + 200 , height/2 + 110, 2, 0, Math.PI*2, false);
	ctx.fill();

	ctx.beginPath();
	ctx.arc(width/2 + 125 , height/2 + 180, 3, 0, Math.PI*2, false);
	ctx.fill();

	ctx.beginPath();
	ctx.arc(width/2 + 120 , height/2 + 225, 2, 0, Math.PI*2, false);
	ctx.fill();

	ctx.beginPath();
	ctx.arc(width/2 + 150 , height/2 + 290, 3, 0, Math.PI*2, false);
	ctx.fill();
	//ctx.fillStyle = "faa";
	//ctx.fillRect(0,0,width, height);
	//ctx.fillStyle = "afa";
	//ctx.fillRect(0,0,width, height/2);
	tapiriik.environs.starfieldAge = tapiriik.environs.days;
};

tapiriik.environs.Draw = function(){

	tapiriik.environs.tod = (new Date()-new Date().setHours(0,0,0,0)) / 86400000;


	var finalCtx = $(tapiriik.environs.canvas).get(0).getContext("2d");
	var ctx = $(tapiriik.environs.fgCanvas).get(0).getContext("2d");
	ctx.clearRect ( 0 , 0 , $(tapiriik.environs.canvas).attr("width")  , $(tapiriik.environs.canvas).attr("height") );
	var width = $(tapiriik.environs.canvas).attr("width") - 20;
	var height = $(tapiriik.environs.canvas).attr("height");
	if (tapiriik.environs.lastTod !== undefined && tapiriik.environs.tod < tapiriik.environs.lastTod){
		tapiriik.environs.days++;
	}
	tapiriik.environs.lastTod = tapiriik.environs.tod;

	var nighttimeBrightness = 1-Math.abs(0.5-tapiriik.environs.moonPhase)*2;
	nightFilter = [5,5,25]
	var sunRad = 100;
	var sunX, sunY;
	var subscale;
	var day, dusk, night;
	if ((tapiriik.environs.tod > tapiriik.environs.sunset && tapiriik.environs.tod < tapiriik.environs.moonrise) || (tapiriik.environs.tod > tapiriik.environs.moonset && tapiriik.environs.tod < tapiriik.environs.sunrise) ) {
		dusk = true;
		day = night = false;
		subscale = tapiriik.environs.tod;
		if (tapiriik.environs.tod < tapiriik.environs.sunrise) {
			subscale -= tapiriik.environs.moonset;
			subscale /= (tapiriik.environs.sunrise - tapiriik.environs.moonset)
		} else {
			subscale -= tapiriik.environs.sunset;
			subscale /= (tapiriik.environs.moonrise - tapiriik.environs.sunset)
		}
	}
	else if (tapiriik.environs.tod < tapiriik.environs.sunrise || tapiriik.environs.tod > tapiriik.environs.sunset){
		subscale = tapiriik.environs.tod;
		if (tapiriik.environs.tod <= tapiriik.environs.moonset){
			subscale += 1-tapiriik.environs.moonrise;
		} else {
			subscale -= tapiriik.environs.moonrise;
		}
		subscale /= tapiriik.environs.moonset + (1-tapiriik.environs.moonrise);
		
		day = dusk = false;
		night = true;
	} else {
		subscale = (tapiriik.environs.tod - tapiriik.environs.sunrise) / tapiriik.environs.daytime;
		day = true;
		dusk = night = false;
	}
	duskFactor = Math.pow(Math.abs(subscale-0.5)*2, 3);
	if (dusk) duskFactor = 1;

	//fill nighttime sky
	daySky = [192, 222, 237]
	nightSky = [17,17,30]
	finalCtx.fillStyle = arrToRgba(arrMix(daySky, nightSky, day?duskFactor:1));
	finalCtx.fillRect(0,0,width+20, height);

	if (duskFactor>0.15 || night){
		finalCtx.save();
		finalCtx.globalAlpha = day?duskFactor:1;
		finalCtx.translate(width/2,150);
		var rot=Math.PI*tapiriik.environs.tod*2;
		finalCtx.rotate(rot);
		finalCtx.drawImage(tapiriik.environs.starCanvas, -width*1.5, -height*1.5)
		debugWrite(ctx, "Starfield rot=" + rot.toFixed(2) +" age="+tapiriik.environs.starfieldAge,150);
		finalCtx.restore();
	}
	

	if (tapiriik.environs.days > tapiriik.environs.starfieldAge && day && subscale > 0.5){
		tapiriik.environs.GenerateStarfield();
	}

	//sun follows elliptical path from bottom left to bottom right, peaking at y=0
	sunTheta = (subscale-0.5) * Math.PI;
	sunY = (Math.pow((subscale*2-1),2)) * height;
	sunX = subscale * (width + 2*sunRad) -sunRad;

	tapiriik.environs.azimuth = 1-(Math.abs(tapiriik.environs.tod - (tapiriik.environs.sunrise + (tapiriik.environs.sunset - tapiriik.environs.sunrise)/2)) / (tapiriik.environs.sunset - tapiriik.environs.sunrise));
	debugWrite(ctx, "TOD=" + tapiriik.environs.tod.toFixed(2) + " sub="+subscale.toFixed(2) + " " + (day?"day":night?"night":"dusk")+ " az=" + tapiriik.environs.azimuth.toFixed(2) +" dusk=" + duskFactor.toFixed(2) + " day=" + tapiriik.environs.days,50);
	debugWrite(ctx,"Moon=" + tapiriik.environs.moonPhase.toFixed(2) +" illum="+nighttimeBrightness.toFixed(2),70);


	if (day){

		ctx.fillStyle = "white";
		ctx.beginPath();
		ctx.arc(sunX, sunY, sunRad, 0, 2 * Math.PI, false);
		ctx.fill();
	} else if (night) {
		sunRad = 50;
		sunX = subscale * (width + 2*sunRad) -sunRad;
		ctx.fillStyle = "#111";
		ctx.beginPath();
		ctx.arc(sunX, sunY, sunRad, 0, 2 * Math.PI, false);
		ctx.fill();
		var compBuffer = renderToCanvas(sunRad*2, sunRad*2, function(compCtx){
			compCtx.fillStyle = "white";
			compCtx.beginPath();
			compCtx.arc(sunRad, sunRad, sunRad, 0, 2 * Math.PI, false);
			compCtx.fill();
			
			

			//ctx.fillStyle = "red";
			//ctx.beginPath();
			
			//ctx.fill();

			var carve = tapiriik.environs.moonPhase; // 0.25 = 0.75 = split
			var subcarve = (carve % 0.5 )*2;
			var osubcarve = subcarve;

				
			//exponential ease in/out
			if (subcarve != 0 && subcarve != 1){
				subcarve *= 2;
				if (subcarve<1) {
					subcarve = 0.5*Math.pow(2,10*(subcarve-1))
				} else {
					subcarve = 0.5*(-Math.pow(2,-10*(subcarve-1)) + 2)
				}
			}
			//linearize near ends
			var endptFactor =(0.5-Math.abs(0.5-subcarve))*2 / 0.01;
			if (endptFactor<1){
				if (subcarve > 0.5){
					subcarve = Math.min(1,subcarve + (1-endptFactor)*0.0007);
				} else {
					subcarve = Math.max(0,subcarve - (1-endptFactor)*0.0007);
				}
			}


			carve = subcarve/2 + (carve>0.5?0.5:0);
			var R = sunRad/Math.pow(Math.abs(1-(carve%0.5)*4),1);

			

			compCtx.save()
			compCtx.globalCompositeOperation = carve <= 0.25 || carve >= 0.75 ?"destination-out" : "destination-in";

			if (R>3000) {
				if ((carve < 0.25 || carve > 0.75) ^ carve < 0.5) {
					compCtx.fillRect(sunRad, 0, sunRad, sunRad*2);	
				} else {
					compCtx.fillRect(0, 0, sunRad, sunRad*2);	
				}
				
			} else {
				var offset = (carve%0.5>0.25 ?1:-1)*Math.sqrt(Math.pow(R, 2) - Math.pow(sunRad, 2))
				
				compCtx.strokeStyle = "red";
				compCtx.beginPath();
				compCtx.arc(sunRad + offset, sunRad, R + (3*Math.abs(0.5-subcarve)),  0, Math.PI*2, false);
				compCtx.fill();
			}

			
			//ctx.lineTo(sunX, sunY - sunRad)
			//ctx.arcTo(sunX + sunRad, sunY, sunX, sunY + sunRad, sunRad)
			//ctx.lineTo(sunX, sunY + sunRad);
			
			//
			//ctx.fillStyle = "white";
			//ctx.fillText("carve=" + carve.toFixed(5) + " sub="+subcarve.toFixed(5)+"/"+osubcarve.toFixed(5),10,70);
			compCtx.restore()
			
			debugWrite(ctx, "  cresc=" + carve.toFixed(2) +" sub="+subcarve.toFixed(2)+"/"+osubcarve.toFixed(2) ,90);
			debugWrite(ctx, "  R="+R.toFixed(2)+" off="+(3*Math.abs(0.5-subcarve)).toFixed(2),110);
		});
		ctx.drawImage(compBuffer, sunX-sunRad, sunY-sunRad);

		

		
		//ctx.fillText("End=" + endptFactor.toFixed(5),10,90);
		
	}

	var shadowOriginX = sunX;
	var shadowOriginY = sunY - 500;

	for (var i = 0; i < tapiriik.environs.mountains.length; i++) {
		mtn = tapiriik.environs.mountains[i];
		

		if (i>0 && day){
			//cast shadows
			var castDir = 1;
			var shadowSlope = (shadowOriginY - (height - mtn.height))/(shadowOriginX - (mtn.offset + mtn.width/2))
			if (shadowOriginX < mtn.offset + mtn.width/2) castDir = -1;

			//var shadowCtx = tapiriik.environs.shadowCanvas.getContext("2d");
			
			//if (Math.abs(shadowSlope)-(mtn.width/mtn.height) > 0) continue; //prevent self-shadowing, tho this is broken
			
			ctx.save();
			ctx.fillStyle = "rgba(0,0,0,0.05)";
			
			ctx.globalCompositeOperation = "source-atop"
			ctx.lineWidth="2"
			ctx.beginPath();
			ctx.moveTo((mtn.offset + mtn.width/2)-5000 * castDir, (height - mtn.height)-5000*shadowSlope * castDir);

			ctx.lineTo(mtn.offset + mtn.width/2, height - mtn.height);

			ctx.lineTo(mtn.offset + (castDir>0?0:mtn.width), height);
			ctx.lineTo((mtn.offset + mtn.width/2)-5000 * castDir, height);
			ctx.fill();

			ctx.restore();
			ctx.fillStyle = "rgba(0,0,0,0.015)";
			ctx.fill();
			
			//ctx.drawImage(tapiriik.environs.shadowCanvas,0,0);

			//ctx.restore();

			
		}
		color = mtn.color;
		nightMixAmnt = 0.95;
		if (day){
			nightMixAmnt *= duskFactor
		}
		color=arrMix(color, nightFilter, nightMixAmnt);
		//color=arrMix(color,[0,0,0], nightVaryAmnt);
		color=arrMix(color,[0,0,0], Math.max(0,Math.pow(duskFactor,day?2:1)));
		nightDamp = Math.min(1,(1-nighttimeBrightness)*(1-duskFactor)*1.5)
		if (!day){
			color=arrMix(color,[0,0,0], nightDamp);	
		} else {
			nightDamp = 0;
		}
		if (i==0){
			
			debugWrite(ctx, "nMix="+nightMixAmnt.toFixed(2) + " nDamp="+nightDamp.toFixed(2),130);
		}
		

		ctx.fillStyle = arrToRgba(color);
		ctx.beginPath();
		ctx.moveTo(mtn.offset, height);
		ctx.lineTo(mtn.offset + mtn.width, height);
		ctx.lineTo(mtn.offset + mtn.width/2, height - mtn.height);
		ctx.lineTo(mtn.offset, height);
		ctx.fill();
		
	};

	finalCtx.drawImage(tapiriik.environs.fgCanvas, 0, 0);

	
};

tapiriik.environs.Resize = function(){
	var width = $(window).width();
	var height = $(window).height()
	$(tapiriik.environs.fgCanvas).attr({width:  width, height: height}); // we add 20 since apparently you can only draw off the canvas with negative coords
	$(tapiriik.environs.canvas).attr({width: width, height: height}); // we add 20 since apparently you can only draw off the canvas with negative coords
	$(tapiriik.environs.starCanvas).attr({width: width*3, height: height*3});
	//$(tapiriik.environs.canvas).attr({width: "500", height: "800"});
	tapiriik.environs.GenerateStarfield();
	tapiriik.environs.Draw();

}
$(document).ready(tapiriik.environs.Init);
$(window).resize(tapiriik.environs.Resize);