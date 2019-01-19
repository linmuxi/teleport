(function () {
      // canvas 实现 watermark
      function __canvasWM({
        // 使用 ES6 的函数默认值方式设置参数的默认取值
        // 具体参见 https://developer.mozilla.org/zh-CN/docs/Web/JavaScript/Reference/Functions/Default_parameters
        container = document.body,
        width = '320',
        height = '80',
        textAlign = 'left',
        textBaseline = 'middle',
        font = "37px microsoft yahei",
        fillStyle = 'rgba(99, 99, 99,0.2)',
		//fillStyle = 'rgba(0,255,0,0.2)',
        content = '请勿外传',
        rotate = '-20',
        zIndex = 99999999
      } = {}) {
        var args = arguments[0];
        var canvas = document.createElement('canvas');
        //canvas.setAttribute('width', width);
        //canvas.setAttribute('height', height);
        var ctx = canvas.getContext("2d");

        //ctx.textAlign = textAlign;
        //ctx.textBaseline = textBaseline;
        ctx.font = font;
        ctx.fillStyle = fillStyle;
        ctx.rotate(Math.PI / 180 * rotate);
        ctx.fillText(content, -30,130);
		
        var base64Url = canvas.toDataURL();
		
		if(document.getElementById("mysy")){
			document.getElementById("mysy").remove();
		}

        const watermarkDiv = document.createElement("div");
		watermarkDiv.setAttribute("id","mysy");
		watermarkDiv.setAttribute("class","");
        watermarkDiv.setAttribute('style', `
          position:absolute;
          top:0;
          left:0;
          width:100%;
          height:100%;
          z-index:${zIndex};
          pointer-events:none;
          background-repeat:repeat;
          background-image:url('${base64Url}')`);

        container.style.position = 'relative';
        container.insertBefore(watermarkDiv, container.firstChild);

		//window.flag = md5(watermarkDiv.getAttribute("style")+""+watermarkDiv.getAttribute("class"));
          window.flag = watermarkDiv.getAttribute("style")+""+watermarkDiv.getAttribute("class");
      };

      window.__canvasWM = __canvasWM;
	  
	  setInterval(function(){
		var mysy = document.getElementById("mysy");
		if(mysy){
			//if(window.flag != md5(mysy.getAttribute("style")+""+mysy.getAttribute("class"))){
            if(window.flag != (mysy.getAttribute("style")+""+mysy.getAttribute("class"))){
				__canvasWM({
				  content: getContent()
				})
			}
		}else{
			__canvasWM({
			  content: getContent()
			})
		}
	  },500);
    })();

    // 调用
    setTimeout(function(){
        __canvasWM({
            content: getContent()
        });
    },500);

	function getContent(){
		var userName = jQuery(".grv-icon-user").attr("title");
		if(userName){
            return userName+"SMY";
        }else{
		    return "wenyangyiSMY";
        }
	}