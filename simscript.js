function getDbpediaInfo(){
          var currVid;
          var artist="";
          var title="";

           $.ajax({
                           url: 'https://www.googleapis.com/youtube/v3/videos?key=' + 'AIzaSyCmxhjyAdTBxuEOG_etapCgLYwIBpSmdbQ' + '&id=' + currentVideo + '&part=snippet',
                           success: function(data){ currVid=data.items[0];},

                           complete: function(){
                                var titolo=cutTitle(currVid.snippet.title);
                                if(titolo.length==2){
                                $.get("http://site1830.tw.cs.unibo.it/abstract?artist="+titolo[0]+"&title="+titolo[1].trim(), function(data, status){
				console.log(data);
                                 if(data.results.bindings[0]){

                                    var wiki=data.results.bindings[0].Sabstract;
                                    var artist=data.results.bindings[0].abstract;
                                    if(data.results.bindings[0].Sabstract){
                                      $('#wiki_container').html(wiki.value);
                                      $('#artist_container').html(artist.value);
                                    }
                                  }
                                    else{
                                      $('#wiki_container').html("no abs founded\n");
                                      $('#artist_container').html("no abs founded\n");
                                    }
                                    
                                });
                                $.get("http://site1830.tw.cs.unibo.it/info?artist="+titolo[0]+"&title="+titolo[1].trim(), function(data, status){
                                  console.log("risposta: ");
                                   
                                    if(data.results.bindings[0]){
                                    var info=data.results.bindings[0];
                                    console.log(info);
				    				if(info.album)
                                        $('#info_container').append("album: <a href= "+info.album.value+">" +info.album.value+"</a><br>");
                                    if(info.artName) 
                                        $('#info_container').append("artista: " +info.artName.value+"<br>");
                                    if(info.relDate)
                                        $('#info_container').append("relDate: " +info.relDate.value+"<br>");
                                    if(info.genre)
										$('#info_container').append( "genre: <a href="+info.genre.value+">"+info.genre.value+"</a><br>");  
				    				if(info.wik)
                                        $('#info_container').append("wiki: " +info.wik.value+"<br>");
                                }else
				 					 $('#info_container').append("no info founded");
                                });
                              }else{
				 					 $('#info_container').append("no info founded");
				 					 $('#wiki_container').html("no abs founded\n");
                                      $('#artist_container').html("no abs founded\n");
                                  }


                               }
           });
         }

function  cutTitle(data){
      	var  title=data;

        title=title.split("-");
        //console.log(title);
        if(title.length==1){

          title=title[0].split("|");

        }if(title.length>1){

           title[0]=title[0].trim();
           title[1]=title[1].split("(");
           //if(title[1].length>=2)
           	title[1]=title[1].shift();
			title[1]=title[1].trim();
           console.log(title);
        }
         // console.log("titolo"+titolo);
         return(title);

         //ritorno un'array dove il primo elemento è il nome dell'artista e il secondo è il titolo della canzone. 
         //voi quando prendete l'oggetto ritornato all'interno della funzione vi dovete prendere oggetto[1].
}

function getArtistSimilarity(category){
   var currVid;
                  var html="";
                  $.ajax({
                      url: 'https://www.googleapis.com/youtube/v3/videos?key=' + 'AIzaSyCmxhjyAdTBxuEOG_etapCgLYwIBpSmdbQ' + '&id=' + currentVideo + '&part=snippet',
                      success: function(data){ currVid=data.items[0];},

                      complete: function(){
  var titolo;
                        titolo=cutTitle(currVid.snippet.title);
                              $.get("http://site1830.tw.cs.unibo.it/Artist?title="+titolo[1].trim(), function(data, status){
                                    var artist="";
                                    var album="";

                                    console.log(data);

                                    console.log("status: "+status);

                                    if(data.results.bindings.length>0){
                                      for(var k=0; k<data.results.bindings.length; k++){
                                        artist=data.results.bindings[k].artName.value;
                                     
                                        console.log("artist: "+artist);
                                        $.get("http://site1830.tw.cs.unibo.it/Song?artist="+artist, function(data, status){
                                         if(data.results.bindings.length>0){
                                            album=data.results.bindings[0].albumName.value;
                                            var date="";
                                            date=data.results.bindings[0].date;
                                            if(data.results.bindings[0].songName){

            for(var j=0;j<data.results.bindings.length;j++){
                if(data.results.bindings[j].songName != "undefined"){
                                                var songs="";
                                                songs=data.results.bindings[j].songName.value;
                                                console.log("songs: ");
                                                console.log(songs);
                                                $.ajax({
                                                  cache: false,
                                                  data: {
                                                    key:'AIzaSyCmxhjyAdTBxuEOG_etapCgLYwIBpSmdbQ',
                                                    q:songs,
                                                    part:'id,snippet',
                                                    maxResults:1},
                                                    dataType:'json',
                                                    type:'GET',
                                                    url:'https://www.googleapis.com/youtube/v3/search'
                                                  })

                                                  .done(function(data) {
                                                    console.log(data.items[0]);

                                                    html += "<div  class='card border-info mb-3' style='width: 16rem;border:none; display: inline-block; padding:1%; padding-top:3%'>";
                                                    html+=" <img class='card-img-top'  src ='https://img.youtube.com/vi/"+data.items[0].id.videoId+"/default.jpg' value='"+data.items[0].id.videoId+"' alt='Card image cap'>";
                                                    html += "<div class='card-body' style=' overflow: hidden' > <p class='card-text'><b>"+ data.items[0].snippet.title +"</b><br>"+data.items[0].snippet.channelTitle+"<br>"+ (data.items[0].snippet.publishedAt).slice(0,-14)+"</br><br>"+artist+"</br><br>"+album+"</br></p></div></div> ";

                                                    $(".tabcontent#"+category).html(html);
                                                    $('img').click(function(){
                                                          caricavideo($(this).attr("value"));
                                                    });
                                                  });
                } else {

              $('#ArtistSimilarity').html('<h1>Sorry</h1><br><h3>Not found!</h3>');

                }
                

                
                                                }
                                              }
                                            }
                                              else {

                                        $('#ArtistSimilarity').html('<h1>Sorry</h1><br><h3>Not found!</h3>');

                                      }
                                          });
                                        }
                                    }
          else {
              $('ArtistSimilarity').html('<h1> Sorry </h1> Not found!');
          }
                                    });
                                  }
                                });
                                 $(".tabcontent#"+category).html(html);
          html="";
                              }


function getGenreSimilarity(category){
  var currVid;
      var html="";
      var artist="";
      $.ajax({
        url: 'https://www.googleapis.com/youtube/v3/videos?key=' + 'AIzaSyCmxhjyAdTBxuEOG_etapCgLYwIBpSmdbQ' + '&id=' + currentVideo + '&part=snippet',
                    success: function(data){ currVid=data.items[0];},

                    complete: function(){
                           var titolo;
                      titolo=cutTitle(currVid.snippet.title);
                          $.get("http://site1830.tw.cs.unibo.it/GenreSimilarity?title="+titolo[1].trim(), function(data, status){
                                  var genere="";
                              if(data.results.bindings.length>0  || data.results.bindings[0].genereNome != "undefined"){
                                      genere=data.results.bindings[0].genereNome.value;

                                      console.log(genere);

                                      $.get("http://site1830.tw.cs.unibo.it/Artist?title="+titolo[1].trim(), function(data, status){
                                          if(data.results.bindings.length>0){
                                              for(var k=0; k<data.results.bindings.length; k++){
              if( data.results.bindings[k].artName != "undefined"){
                                                  artist=data.results.bindings[k].artName.value;
                                                  console.log("artist:"+artist);
                                                  $.get("http://site1830.tw.cs.unibo.it/Genre?genere="+genere+"&artist"+artist, function(data, status){

                                                          if(data.results.bindings[0].genereNome){
                                                              for(var j=0;j<recommender_size;j++){
                                                                  var songs="";
                                                                  var gen="";
                                                                  gen=data.results.bindings[0].genereNome.value;
                                                                  songs=data.results.bindings[j].songName.value;
                                                                  console.log("songs: ");
                                                                  console.log(songs);
                                                                  $.ajax({
                                                                      cache: false,
                                                                      data: {
                                                                        key:'AIzaSyCmxhjyAdTBxuEOG_etapCgLYwIBpSmdbQ',
                                                                        q:songs,
                                                                        part:'id,snippet',
                                                                        maxResults:1},
                                                                        dataType:'json',
                                                                        type:'GET',
                                                                        url:'https://www.googleapis.com/youtube/v3/search'
                                                                      })

                                                                      .done(function(data) {
                                                                          console.log(data.items[0]);

                                                                          html += "<div  class='card border-info mb-3' style='width: 16rem; border:none; display: inline-block; padding:1%; padding-top:3%'>";
                                                                          html+=" <img class='card-img-top'  src ='https://img.youtube.com/vi/"+data.items[0].id.videoId+"/default.jpg' value='"+data.items[0].id.videoId+"' alt='Card image cap'>";
                                                                          html += "<div class='card-body' style=' overflow: hidden'> <p class='card-text'><b>"+ data.items[0].snippet.title +"</b><br>"+data.items[0].snippet.channelTitle+"<br>"+ (data.items[0].snippet.publishedAt).slice(0,-14)+"</br><br>"+gen+"</br></p></div></div> ";

                                                                          //console.log(html);
                                                                          $(".tabcontent#"+category).html(html);
                                                                          $('img').click(function(){
                                                                                caricavideo($(this).attr("value"));
                                                                          });
                                                                      });
                                                              }
                }
              });
              }
              else {
                  $('#GenreSimilarity').html('<h1>Sorry</h1><br><h3>Not found!</h3>');
              }
                }
            }
            else {
                $('#GenreSimilarity').html('<h1>Sorry</h1><br><h3>Not found!</h3>');
            }
              });
            }
        });
        }
      });
       $(".tabcontent#"+category).html(html);
       html="";
}

function getBandSimilarity(category){
  var html="";
    $.ajax({
        url: 'https://www.googleapis.com/youtube/v3/videos?key=' + 'AIzaSyCmxhjyAdTBxuEOG_etapCgLYwIBpSmdbQ' + '&id=' + currentVideo + '&part=snippet',
                success: function(data){ currVid=data.items[0];},

                complete: function(){
                     var titolo;
                  titolo=cutTitle(currVid.snippet.title);
                   $.get("http://site1830.tw.cs.unibo.it/BandSimilarity?title="+titolo[1].trim(), function(data, status){

                            var band="";
                       if(data.results.bindings[0].bandName >0 || data.results.bindings[0].bandName != "undefined"){
                                band=data.results.bindings[0].bandName.value;
                                console.log(data.results.bindings[0].bandName.value);
                                $.get("http://site1830.tw.cs.unibo.it/GenreMembri?band="+band, function(data, status){
                                    var membri="";
                                    console.log(data.results.bindings.length);
                                    //  if(data.results.bindings[0]){
                                    var k=0;
                                    for(k=0 ; k<data.results.bindings.length ; k++){
        
                                        membri=data.results.bindings[k].memberName.value;
                                        console.log(membri);
                                        console.log(band);
                                        $.get("http://site1830.tw.cs.unibo.it/Band?membriBand="+membri+"&band="+band, function(data, status){
                                            var songs="";
                                            var nomeBand="";
                                            console.log(data.results.bindings.length);

                                            if(data.results.bindings.length>0){
                                                for(var j=0;j<data.results.bindings.length;j++){
                if(data.results.bindings[j].songName != "undefined"){

                                                    songs=data.results.bindings[j].songName.value;
                                                    nomeBand=data.results.bindings[j].bandName.value;
                                                    //console.log("songs: ");
                                                    console.log(songs);
                                                    $.ajax({
                                                        cache: false,
                                                        data: {
                                                          key:'AIzaSyCmxhjyAdTBxuEOG_etapCgLYwIBpSmdbQ',
                                                          q:songs,
                                                          part:'id,snippet',
                                                          maxResults:1},
                                                          dataType:'json',
                                                          type:'GET',
                                                          url:'https://www.googleapis.com/youtube/v3/search'
                                                        })

                                                        .done(function(data) {
                                                          console.log(data.items[0]);

                                                          html += "<div  class='card border-info mb-3' style='width: 16rem; border:none; display: inline-block; padding:1%; padding-top:3%'>";
                                                          html+=" <img class='card-img-top'  src ='https://img.youtube.com/vi/"+data.items[0].id.videoId+"/default.jpg' value='"+data.items[0].id.videoId+"' alt='Card image cap'>";
                                                          html += "<div class='card-body' style=' overflow: hidden'> <p class='card-text'><b>"+ data.items[0].snippet.title +"</b><br>"+data.items[0].snippet.channelTitle+"<br>"+ (data.items[0].snippet.publishedAt).slice(0,-14)+"</br><br>"+membri+"</br><br>"+nomeBand+"</p></div></div> ";

                                                          //console.log(html);
                                                          $(".tabcontent#"+category).html(html);
                                                          $('img').click(function(){
                                                                caricavideo($(this).attr("value"));
                                                          });
                                                        });
                }
                else {
              $('#BandSimilarity').html('<h1>Sorry</h1><br><h3>Not found!</h3>');
                }
                                                    }
                                            }
              else {
            $('#BandSimilarity').html('<h1>Sorry</h1><br><h3>Not found!</h3>');
              }
                                                  });
                                                }
                                              });
                                            }
                              else {

                                        $('#BandSimilarity').html('<h1>Sorry</h1><br><h3>Not found!</h3>');

                                      }
                                          });
                                        }

                                    });
                                  $(".tabcontent#"+category).html(html);
                                  html="";
}

function getRelated(vid,dim,info,category){
  var request_related = gapi.client.request({
              'method': 'get',
              'path': '/youtube/v3/search',
              'params':{
                'part': 'snippet',
                'relatedToVideoId': currentVideo,
                'type': 'video',
                'maxResults':recommender_size
              }
              });
              request_related.execute(function(response){
               // resRel=response;
                for(i=0;i<recommender_size;i++){
                  vid[i]=response.items[i];
                }
                stampa(vid,dim,category,info);
  
              });
}
function makeid() {
            var text = "";
            var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            var possible1 = "abcdefghijklmnopqrstuvwxyz";
            var possible2 = "0123456789";
            text = possible.charAt(Math.floor(Math.random() * possible.length));
            text1 = possible1.charAt(Math.floor(Math.random() * possible.length));
            text2 = possible2.charAt(Math.floor(Math.random() * possible.length));
            return (text+text1+text2);
        }

function getRandom(vid,dim,info,category){
  
   var q = makeid();
                 console.log("random");
                 var request = gapi.client.youtube.search.list({
                    q:q,
                    part: 'snippet',
                    type: 'video',                //servono per far cercare soltanto video musicali
                    videoCategoryId: '10',
                    maxResults: 50
            });
            request.execute(function(response){
            
              var rnd=Math.floor(Math.random()*response.items.length);
              //resRand=response;
             // $('#random-button').click(caricaTab('Random'));
               console.log(response);
               dim=20;
               var k=0;
              for (var i = 0; i < 20; i++) {
                  vid[i]=response.items[i];
              }
              stampa(vid,dim,category,info);
            });
}

function getFvitali(vid,dim,info,category){
   var Fvit;
            $.ajax({

              url: "http://site1825.tw.cs.unibo.it/TW/globpop?"+"?id="+currentVideo,
              success: function(data) {
                Fvit=data;
                //$('#Fvitali-button').click(caricaTab('Fvitali'));

              },
              complete: function(){
                dim=Fvit.recommended.length;
              for (var i = 0; i <dim; i++) {
                vid[i]=Fvit.recommended[i].videoID;

              } 
              info =false;
              var j=0;
                  takeInfoById(vid,dim,category,info,j);
            },
              error: function(data) {
                alert("Caricamento impossibile");
              }
            });

}
