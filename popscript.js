      function savePopularity(){
        $.get("http://site1830.tw.cs.unibo.it/popularity?to_id="+currentVideo+"&reason="+current_reason, function(data, status){

        });
        var vid_recent=[recommender_size];
        var size=caricarecent(vid_recent);
        var flag=false;
        if(lastVideo!=currentVideo)
          for(var i=0;i<size;i++){
            if(lastVideo==vid_recent[i]){
              $.get("http://site1830.tw.cs.unibo.it/popularity?from_id="+lastVideo+"&to_id="+currentVideo+"&reason="+current_reason, function(data, status){
                    //alert("last video: " + lastVideo + "\this video: " + currentVideo);
              });
            }
    }
      }
      function caricaPopularity(ask,vid){
        var prom=new Promise(function(resolve,reject){
          $.get("http://site1830.tw.cs.unibo.it/popularity?from_id="+currentVideo+"&ask="+ask, function(data, status){
              var v=JSON.parse(data);
              for(var i = 0;i<v.length;i++)
              vid.push(v[i].to_id);
		//alert("carica popularity");
	//	alert(data); resolve(6);
              resolve(v.length);
          });
        });
        return prom;
      }
      function saveReason(category){
	var out="undefined";
        if((category=="popLocAss")||(category=="popLocRel")){
          out="LocalPopularity";
        }else if((category=="popGlobAss")||(category=="popGlobRel")){
          out="GlobalPopularity";
        }else out=category;	
 	 last_reason=out;
      }
      function timedCount(){
        time=time+1;
        if(time==10){ //se il viddeo e' stato visto
          salvarecent(currentVideo);
        savePopularity();

        }
        if(time<10)
          clock=setTimeout(timedCount, 1000);
       }


      function startClock(){
        if(!timerOn){
          timerOn=1;
          timedCount();
        }
      }
      function newClock(){
        timerOn=0;
        time=0;

      }
      function stopClock(){
        clearTimeout(clock);
        timerOn=0;

      }
      function historyBack(id){
            newClock();
      lastVideo=null;
            currentVideo=id;
            player.loadVideoById(id, 0, "large");  //"0" secondi di inizio del video
        }
        //salva il video ID come ultimo video visualizzato
        function salvarecent(ID){
          var vid=[recommender_size];
          var dim=caricarecent(vid);
          var exist=false;
          for (var i = 0; i <dim; i++) {
           if(ID==vid[i])
              exist=true;
          }

          if(!exist){
                var key=localStorage.getItem("counter");  //counter=valore della prossima chiave libera 
                if (!key)    //se non esiste già
                  key=0;  
                localStorage.setItem(key, ID);
                key++;
                key=key%recommender_size;
                localStorage.setItem("counter", key);
          }
        }
        //carica una lista con gli ultimi video visualizzati
        function caricarecent(vid){
          var i;
          for(i=0;i<recommender_size;i++){
            key=(localStorage.getItem("counter")-i-1+recommender_size)%recommender_size;
            var video=localStorage.getItem(key);
            if (video==null)
              return i;   //dimensione di out
            vid.push(video);     //returna null se l'eselmento non esiste
            //out.push(video);
          }
          return recommender_size;
        }
