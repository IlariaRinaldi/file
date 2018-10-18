var fs = require('fs');
var http = require('http');

var url = require("url");			//permette di caricare anche gli script del file html
//var mb = require('musicbrainz');
const request = require('request');
var path =require('path');

//var locpop=require("popularity0.5.js");
const express=require('express');
const app=express();
var locpop = require (__dirname+'/popularity0.7.js');
var globpop = require (__dirname+'/popularity_globale0.7.js');
var reset=false;		//se true, resetta il DB e non lo aggiorna
//locpop.debugDB(-3);				//se false, si mette ad aggiornare il DB di continuo
//app.use(express.static(path.join(__dirname,'static')));
//locpop.reset();
//globpop.debugCicle();

app.get('/', function(req,res){
		console.log("i'm serving!");
		fs.readFile(__dirname+'/alpha.html',function(err,data){
	          res.writeHead(200, {'Content-Type' : 'text/html'});
                  res.end(data);
                });
});
app.get('/vale.jpeg',(req,res)=>{
	script = fs.readFileSync(__dirname+"/vale.jpeg");
        res.send(script);
});
app.get('/IlaImmagine.jpg',(req,res)=>{
        script = fs.readFileSync(__dirname+"/IlaImmagine.jpg");
        res.send(script);
});

app.get('/chiaImmagine.jpg',(req,res)=>{
        script = fs.readFileSync(__dirname+"/chiaImmagine.jpg");
        res.send(script);
});
app.get('/luciano.jpeg',(req,res)=>{
        script = fs.readFileSync(__dirname+"/luciano.jpeg");
        res.send(script);
});

app.get('/logo1.jpg',(req,res)=>{
    script = fs.readFileSync(__dirname+"/logo1.jpg");
    res.send(script);
});




app.get('/script1.js',(req,res)=>{
	script = fs.readFileSync(__dirname+"/script1.js", "utf8");
        res.send(script);
});
app.get('/var.js',(req,res)=>{
//        res.writeHead(200, {'Content-Type' : 'text/javascript'});

	script = fs.readFileSync(__dirname+"/var.js", "utf8");
        res.send(script);
});
app.get('/simscript.js',(req,res)=>{
	 script = fs.readFileSync(__dirname+"/simscript.js", "utf8");
        res.send(script);
});
app.get('/popscript.js',(req,res)=>{
	 script = fs.readFileSync(__dirname+"/popscript.js", "utf8");
        res.send(script);
});

app.use('/alpha.html', express.static(__dirname + '/vale.jpeg'));
app.use('/alpha.html', express.static(__dirname + '/luciano.jpeg'));
app.use('/alpha.html', express.static(__dirname + '/IlaImmagine.jpg'));
app.use('/alpha.html', express.static(__dirname + '/chiaImmagine.jpg'));
app.use('/alpha.html', express.static(__dirname + '/logo1.jpg'));


app.get('/globpop',(req,res)=>{
	var id = req.query.id;
	var prom;
	if(id==null){
		prom=locpop.findApiAss();
	}else{
		prom=locpop.findApiRel(id);
	}
	prom.then(function(data){
		var array=[];
		var i;
		var video;
		var last_watched;
		
		var prom2=locpop.findOne(id,true);
		prom2.then(function(vid){
			if((vid!=null)&&(vid.length>0))
				last_watched=vid[0].last_watched.toUTCString();
			else
				last_watched=null;
			for(i=0;i<data.length;i++){
				
				var d=new Date(data[i].last_watched);
				video={
					videoId:data[i].to_id,
					timesWatched:data[i].num,
					prevalentReason:data[i].reason,
					lastSelected:d.toUTCString()
				};
				array.push(video);
			}
			var myobj={
				site:"http://site1830.tw.cs.unibo.it/",
				recommender:id,
				lastWatched:last_watched,
				recommended:array
			};
			res.end(JSON.stringify(myobj));
		});//*/res.end("pippo");
	});//*/res.end("globpop!!!");
});
app.get('/popularity',(req,res)=>{
	console.log("pippo");
	
	var from_id = req.query.from_id;
	var to_id = req.query.to_id;
	var reason=req.query.reason;
	var ask=req.query.ask;
	console.log("from_id: "+from_id+" to_id: "+to_id+" reason: "+reason+" ask: "+ask);
	if(ask!=null){
		var prom;
		if(ask=="popLocAss"){
			prom=locpop.findLocAss();
		}
		if(ask=="popLocRel"){
			prom=locpop.findLocRel(from_id);
		}
		if(ask=="popGlobAss"){
			prom=locpop.findGlobAss();
		}
		if(ask=="popGlobRel"){
			prom=locpop.findGlobRel(from_id);
		}
		prom.then(function(data){
			res.end(JSON.stringify(data));
		});
	}else{
		if(to_id != null){
			if(from_id == null){
				locpop.insertLocAss(to_id,reason);
			}else{
				locpop.insertLocRel(from_id,to_id,reason);
			}
		}
		res.end();
	}
    });

app.get('/abstract', (req,res)=>{
	//var q = url.parse(request.url, true).query;
		var artista= req.query.artist;
		var title=req.query.title;
//		var Release = mb.Release;

    			//PER IL MOMENTO CERCA SOLTANTO PER ARTISTA, FRA POCO FACCIO IN MODO CHE CERCHI ANCHE PER BAND E MUSICALARTIST
    			//HO PRESO I RISULTATI solo IN INGLESE, 
    			var url = "http://dbpedia.org/sparql";
              
				
				var query=`PREFIX dbo: <http://dbpedia.org/ontology/>
							PREFIX foaf:<http://xmlns.com/foaf/0.1/>
							SELECT distinct * WHERE {
							{
							    ?artist a dbo:MusicalArtist.
							    ?artist foaf:name ?artName.
							    ?artist dbo:abstract ?abstract .
							}UNION{
							    ?artist a dbo:Band.
							    ?artist foaf:name ?artName.
							    ?artist dbo:abstract ?abstract  
							}.
							{
							    ?song dbo:musicalArtist ?artist.
							    ?song   dbo:abstract ?Sabstract .
							    ?song   foaf:name ?songName.
							}UNION{
							    ?song dbo:artist ?artist.
							    ?song   dbo:abstract ?Sabstract .
							    ?song   foaf:name ?songName.
							} .
							filter  regex(?artName, "`+artista+`", 'i')
							FILTER  ( langMatches(lang(?abstract), 'en'))
							FILTER  ( langMatches(lang(?Sabstract), 'en'))
							filter  regex(?songName,"`+title+`", 'i')
							}`;

				var queryUrl = encodeURI( url+"?query="+query );
							//console.log(queryUrl);



request.get(queryUrl, { json: true }, (err, res1, body) => {
  if (err) { return console.log(err); }
  //console.log(res1.statusCode);
 if(res1.statusCode){
  	console.log(res1.statusCode);
  	res.json(body);
  }
});



	});
	app.get('/info', (req,res)=>{
	//var q = url.parse(request.url, true).query;
		var artista= req.query.artist;
		var title=req.query.title;
		var info="";

    			//PER IL MOMENTO CERCA SOLTANTO PER ARTISTA, FRA POCO FACCIO IN MODO CHE CERCHI ANCHE PER BAND E MUSICALARTIST
    			//HO PRESO I RISULTATI solo IN INGLESE, 
    			var url = "http://dbpedia.org/sparql";
              
				
				var query=` PREFIX dbo: <http://dbpedia.org/ontology/>
							PREFIX dbp: <http://dbpedia.org/property/>
							PREFIX foaf:<http://xmlns.com/foaf/0.1/>
							SELECT distinct ?artName ?genre ?songName ?relDate ?album ?len WHERE {
							?artist foaf:name ?artName.
							?song   foaf:name ?songName.
							{
							  ?song dbo:musicalArtist ?artist.
							  OPTIONAL {?song dbo:genre ?genre.}
							  OPTIONAL {?song dbo:album ?album.}
							  OPTIONAL {?song dbo:releaseDate ?relDate.}
							  OPTIONAL {?song dbp:length ?len.}
							  OPTIONAL {?song foaf:primaryTopic  ?wik.}
							  

							}UNION{
							  ?song dbo:artist ?artist.
							  OPTIONAL {?song dbo:genre ?genre.}
							  OPTIONAL {?song dbo:album ?album.}
							  OPTIONAL {?song dbo:releaseDate ?relDate.}
							  OPTIONAL {?song dbp:length ?len.}
							  OPTIONAL {?song foaf:primaryTopicOf  ?wik.}
							  
							} 
							.
							filter  regex(?artName, "`+artista+`", 'i')
							filter  regex(?songName, "`+title+`", 'i')
							}`;



//artist non contiene informazioni quali genere,data pubbl ecc..
//id prendi da youtube

				var queryUrl = encodeURI( url+"?query="+query );
							//console.log(queryUrl);



request.get(queryUrl, { json: true }, (err, res1, body) => {
  if (err) { return console.log(err); }
  //console.log(res1.statusCode);
 if(res1.statusCode){
  	console.log("res1.statusCode");
  	res.json(body);
  }

  
});
});

//questa query ritorna l'artista della canzone dato il titolo
app.get('/Artist',(req,res)=>{
	var title=req.query.title;
		var url = "http://dbpedia.org/sparql";
		var Artist= `PREFIX dbo: <http://dbpedia.org/ontology/>
								PREFIX foaf:<http://xmlns.com/foaf/0.1/>
									SELECT  distinct ?artName  WHERE{
										{
												?artist a <http://dbpedia.org/ontology/MusicalArtist>.
												?artist foaf:name ?artName.
												?song a <http://dbpedia.org/ontology/MusicalWork>.
												?song foaf:name ?songName.
												?song dbo:artist ?artist.
											}UNION
											{
												?artist a <http://dbpedia.org/ontology/MusicalArtist>.
												?artist foaf:name ?artName.
												?song a <http://dbpedia.org/ontology/MusicalWork>.
												?song foaf:name ?songName.
												?song dbo:musicalArtist ?artist.
											}
											filter (str(?songName)= "`+title+`")
										}`;

			console.log("artista: "+Artist);
			var queryUrlSimilarityA = encodeURI( url+"?query="+Artist);
			//console.log( queryUrlSimilarityA);
			request.get(queryUrlSimilarityA, { json: true }, (err, res1,body) => {
			  if (err) { return console.log(err); }
			  //console.log(res1.statusCode);
			 if(res1.statusCode){
			  	console.log("body: ");
					console.log(body);
			  	res.json(body);
			  }


			});
});

//questa query ritorna la lista di tutte le canzoni di un'artista
app.get('/Song',(req,res)=>{
		var artist=req.query.artist;
		console.log("song:"+artist);
		var url = "http://dbpedia.org/sparql";
		var queryArtist=`PREFIX dbo: <http://dbpedia.org/ontology/>
											SELECT distinct ?songName ?albumName ?date WHERE {
												?artist a <http://dbpedia.org/ontology/MusicalArtist>.
												?artist <http://xmlns.com/foaf/0.1/name> ?artName.
												?artist dbo:abstract ?abstract .
												?song <http://dbpedia.org/ontology/artist> ?artist.
												?song dbo:abstract ?Sabstract .
												?song dbo:album ?album.
												?song dbo:releaseDate ?date.
												?song <http://xmlns.com/foaf/0.1/name> ?songName.
												?album <http://xmlns.com/foaf/0.1/name> ?albumName.
												filter regex(?artName,'`+ artist +`', 'i')
												filter ( langMatches(lang(?abstract), 'it'))
											}
											ORDER BY ?album DESC(?date) `;
		console.log("song:"+queryArtist);
		var queryUrlSimilarityA = encodeURI( url+"?query="+ queryArtist);

		request.get(queryUrlSimilarityA, { json: true }, (err, res1,body) => {
			  	if (err) { return console.log(err); }
			  	console.log(res1.statusCode);
			 	if(res1.statusCode){
			  		console.log(body);
			  			res.json(body);
			  }
			});
});

//genere similarity
//questa query restituisce il genere della canzone dato il titolo di essa
app.get('/GenreSimilarity',(req,res)=>{
		var title=req.query.title;
		var url = "http://dbpedia.org/sparql";
		var genere= `PREFIX dbo: <http://dbpedia.org/ontology/>
										SELECT ?genereNome ?artName WHERE{
											?song a <http://dbpedia.org/ontology/MusicalWork>.
											?song <http://xmlns.com/foaf/0.1/name> ?songName.
											?song dbo:genre ?genere.
											?genere <http://xmlns.com/foaf/0.1/name> ?genereNome.
											FILTER regex(?songName, "`+ title +`")
										}`;
			console.log("genere"+genere);
			var queryUrlSimilarityG = encodeURI( url+"?query="+genere);
			//console.log( queryUrlSimilarityA);
			request.get(queryUrlSimilarityG, { json: true }, (err, res1,body) => {
			  if (err) { return console.log(err); }
			  //console.log(res1.statusCode);
			 if(res1.statusCode){
			  	console.log("body: ");
				console.log(body);
			  	res.json(body);
			  }
			});
});

//questa query ritorna la lista di canzoni in base al genere ma di artisti diversi dall'artista ritorna precedentemente
app.get('/Genre',(req,res)=>{
	//var title=req.query.title;


	 var genere=req.query.genere;
	var  artist=req.query.artist;
	 console.log("artista:  "+artist);
		var url = "http://dbpedia.org/sparql";
		var queryGenre=`PREFIX dbo: <http://dbpedia.org/ontology/>
												SELECT ?songName ?artistName ?genereNome WHERE{
													?song a <http://dbpedia.org/ontology/MusicalWork>.
													?song <http://xmlns.com/foaf/0.1/name> ?songName.
													?song dbo:genre ?genere.
													?genere <http://xmlns.com/foaf/0.1/name> ?genereNome.
													?song dbo:artist ?artist.
													?artist <http://xmlns.com/foaf/0.1/name> ?artistName.
													FILTER regex(?genereNome, "`+ genere +`").
													FILTER ( ?artistName != "`+ artist+`")
												}`;
			console.log("query genere"+queryGenre);
			var queryUrlSimilarityG = encodeURI( url+"?query="+queryGenre);

			request.get(queryUrlSimilarityG, { json: true }, (err, res1,body) => {
			  if (err) { return console.log(err); }
			 if(res1.statusCode){
			  	console.log("body: ");
				console.log(body);
			  	res.json(body);
			  }


			});
});

//band similarity

//questa query ritorna il nome della band che suona la canzone
app.get('/BandSimilarity',(req,res)=>{
	var title=req.query.title;
		var url = "http://dbpedia.org/sparql";
		var band=`PREFIX dbo: <http://dbpedia.org/ontology/>
									SELECT  ?bandName WHERE{
										?song a <http://dbpedia.org/ontology/MusicalWork>.
										?song <http://xmlns.com/foaf/0.1/name> ?songName.
										?song dbo:musicalBand ?band.
										?band  <http://xmlns.com/foaf/0.1/name>  ?bandName.
										FILTER regex(?songName, "`+title+`").
										}`;

			var queryUrlSimilarityB = encodeURI( url+"?query="+band);
			console.log( band);
			request.get(queryUrlSimilarityB, { json: true }, (err, res1,body) => {
			  if (err) { return console.log(err); }
			  //console.log(res1.statusCode);
			 if(res1.statusCode){
			  	console.log("body: ");
				console.log(body);
			  	res.json(body);
			  }


			});
});


//questa query ritorna i nomi dei membri della band
app.get('/GenreMembri',(req,res)=>{
	var band=req.query.band;
		var url = "http://dbpedia.org/sparql";
		var membriBand=` PREFIX dbo:<http://dbpedia.org/ontology/>
										PREFIX dbp:<http://dbpedia.org/property/>
										PREFIX foaf:<http://xmlns.com/foaf/0.1/>
										SELECT distinct ?memberName WHERE{
											{
												?artist a dbo:Band.
											}
											UNION{
												?artist a dbo:Band.
												?artist foaf:name ?artName.
												OPTIONAL{?artist dbo:bandMember ?member.}
												OPTIONAL{?artist dbo:formerBandMember ?member.}
												?member foaf:name ?memberName
											}
											FILTER regex(?artName,"`+ band +`")
										}`;

			var queryUrlSimilarityB = encodeURI( url+"?query="+membriBand);
			console.log(membriBand);
			request.get(queryUrlSimilarityB, { json: true }, (err, res1,body) => {
			  if (err) { return console.log(err); }
			 if(res1.statusCode){
			  	console.log("body: ");
				console.log(body);
			  	res.json(body);
			  }


			});
});

//questa query ritorna la lista di tutte le canzoni dove un membro della band suona/canta ma in un'altra band
app.get('/Band',(req,res)=>{
	//var title=req.query.title;
	var membriBand=req.query.membriBand;
	console.log("membri"+ membriBand);
	var band=req.query.band;
	var url = "http://dbpedia.org/sparql";
						var queryBand= `PREFIX dbo: <http://dbpedia.org/ontology/>
									PREFIX foaf:<http://xmlns.com/foaf/0.1/>
									SELECT  distinct ?songName ?bandName WHERE{
										{
												?artist a <http://dbpedia.org/ontology/MusicalArtist>.
												?artist foaf:name ?artName.
												?band foaf:name ?bandName.
												?band dbo:formerBandMember ?artist.
												?song a <http://dbpedia.org/ontology/MusicalWork>.
												?song foaf:name ?songName.
												?song dbo:musicalBand ?band.
											}
											UNION
											{
												?artist a <http://dbpedia.org/ontology/MusicalArtist>.
												?artist foaf:name ?artName.
												?band foaf:name ?bandName.
												?band dbo:bandMember ?artist.
												?song a <http://dbpedia.org/ontology/MusicalWork>.
												?song foaf:name ?songName.
												?song dbo:musicalBand ?band.
											}
											FILTER regex(?artName, "`+membriBand+`").
											FILTER ( str(?bandName) != "`+band+`")
										}`;


			var queryUrlSimilarityB = encodeURI( url+"?query="+queryBand);
			console.log(queryBand);
			request.get(queryUrlSimilarityB, { json: true }, (err, res1,body) => {
			  if (err) { return console.log(err); }
			 if(res1.statusCode){
			  	console.log("body: ");
				console.log(body);
			  	res.json(body);
			  }


			});
});

app.listen(8000, ()=>console.log("Listening on 8000"));



	//locpop.debugDB(-2);
	//globpop.keepUpdatingAss();
	//globpop.debugCicle();

