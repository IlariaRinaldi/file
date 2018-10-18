var MongoClient = require('mongodb').MongoClient;
var locpop = require (__dirname+'/popularity0.8.js');
var https = require('http');
var url = "mongodb+srv://tecweb:OOyytUjMGXsM8o6z@tecweb-x51d4.gcp.mongodb.net?retryWrites=true";
//var url="mongodb://localhost:27017";
var gruppi=locpop.gruppi;
var debug=false;
var reasons=locpop.reasons;		
var period_time=1;//50000;
var cicli_fatti=0;
var cicle_time;
var last_cicle_time;
//locpop.reset();
//preUpdate();
//locpop.find_ass("globAss",200);
//save("globAss","serverAss");
//locpop.find_ass("serverAss",200);
//keep_update();

//updateGruppiAss(0,0,);
//locpop.find_ass("1828",200);
//saveAss(0);
//locpop.debugDB(-1);
//keepUpdatingAss();
exports.keepUpdatingAss = keepUpdatingAss;
exports.debugCicle = debugCicle;
locpop.debugDB(-1);
//debugCicle();
function debugCicle(){
	var prom1=locpop.debugDB(cicli_fatti);
	prom1.then(function(){
		cicli_fatti=cicli_fatti+1;
		console.log(cicli_fatti);
		debugCicle();
	});
}
//si autochiama continuamente, perr tenere aggiornato il DB, scaricando i dati degli altri utenti (updateGruppi e updateRel)
//ed elaborandoli (updateAll)
function keepUpdatingAss(){
	if(locpop.isUpdating()){
		setTimeout(function() {
			if(debug) console.log("!!! is updating !!!");
			keepUpdatingAss();
		}, 1000);
	}else{
		last_cicle_time=cicle_time;
		cicle_time=new Date();
		/*if(debug)*/ console.log("starting new cicle");
		setTimeout(function(){
			if(debug) console.log("!!! is NOT updating !!!");
			if(debug) console.log("in keepUpdatingAss()");
			locpop.updateAll();
			var prom= updateGruppi(null,0,true);
			prom.then(function(){
				var prom1= updateRel(0);
				prom1.then(function(){
					if(debug) console.log("nuovo ciclo!");
					//dovrei metterci anche updateRel() qui
					cicli_fatti=cicli_fatti+1;
					locpop.debugDB(cicli_fatti);		//a scopo di debug
					keepUpdatingAss();
				});
			});
		},period_time*1000);
	}
}

//scarica tutti i video relativi a video gia' visti, da tutti gli altri gruppi
//i: il gruppo da cui sta scaricando, deve essere inizializzato a 0
function updateRel(i){
	if(debug) console.log("in updateRel");
	var prom=new Promise(function(resolve,reject){
		if(i<gruppi.length){
			var prom2= locpop.findVidGrup(i);		//prendo gli id dei video visualizzati dal gruppo i-esimo
			prom2.then(function(data){
				var prom3= updateRelRec(data,0,i);
				prom3.then(function(){
					var prom4= updateRel(i+1);
					prom4.then(function(){
						resolve();
					});
				});
			});
		}else
			resolve();
	});
	return prom;
}

//per ogni id contenuto in data, scarica i video relativi a tale id
//i: video di data che sta venendo preso in considerazione, va inizializzato con 0
//gruppo: gruppo da cui stanno venendo prese le info
function updateRelRec(data,i,gruppo){
	if(debug) console.log("in updateRelRec");
	var prom=new Promise(function(resolve,reject){
		if(i<data.length){
			var prom2= updateGruppi(data[i].to_id,gruppo,false);
			prom2.then(function(){
				var prom3= updateRelRec(data,i+1,gruppo);
				prom3.then(function(){
					resolve();
				});
			});
		}else{
			resolve();
		}
		
	});
	return prom;
}

//fa l'update dei video relativi a from_id (ass se from_id==null)
//i: i-esimo gruppo dal quale scaricare i dati
//rec indica se debbano essere cercati video anche nei gruppi successivi (ricorsivamente) o meno
function updateGruppi(from_id,i,rec){
	var prom=new Promise(function(resolve,reject){
		if(debug) console.log("in updateGruppi("+from_id+","+i+")");
		if(i<gruppi.length){
			var uri;
			if(from_id==null)
				uri="http://site"+gruppi[i]+".tw.cs.unibo.it/globpop";
			else
				uri="http://site"+gruppi[i]+".tw.cs.unibo.it/globpop?id="+from_id;
			//console.log(uri);
			https.get(uri, (resp) => {
		  		var data = '';
					  	// A chunk of data has been recieved.
			 	resp.on('data', (chunk) => {
			   		data += chunk;
			 	});
		
				// The whole response has been received. Print out the result.
				resp.on('end', () => {
					try {
						var obj=JSON.parse(data);
						//updateGruppiAss(0,i,obj.recommended);
						if(obj.recommended!=undefined){
							var prom1=insertData(from_id,0,i,obj.recommended);
							prom1.then(function(){
								if(rec){
									prom2=updateGruppi(from_id,i+1,rec);
									prom2.then(function(){
										resolve();
									});
								}else{
									resolve();
								}
							});
						}else{
							resolve();
						}
						
						//console.log(obj.recommended);
						if(debug) console.log("ended");
					}catch(err) {
						if(debug) console.log("errore collegamento ad api "+gruppi[i]+ ": "+err);
						if(rec){
							var prom1=updateGruppi(from_id,i+1,rec);
							prom1.then(function(){
								resolve();
							});
							
						}else
							resolve();
						
					}
				});
		
			}).on("error", (err) => {
				var prom1=updateGruppi(from_id,i+1,rec);
				prom1.then(function(){
					resolve();
				});
				console.log("Error: " + err.message);
			});
		}else{
			if(debug) console.log("updateGruppi finished!");
			resolve();
		}
	});
	return prom;
	
}
//inserisce data nel database
function insertData(from_id,i,j,data){
//i=elemento di data che sta venendo inserito
//j=gruppo che sta inserendo
//se e' relativo ad un video, gli elementi di data sono stati visualizzati dopo from_id, altrimenti
//	from_id sara' null.
	var prom=new Promise(function(resolve,reject){
		if(debug) console.log("in insertData() i:"+i+" j:"+j);
		if(i<data.length){
		
			var id="";
			if(data[i].videoId!=null)
				id=data[i].videoId;
			else
				if(data[i].videoID!=null)
					id=data[i].videoID;
			var num=0;
			if(data[i].timesWatched!=null)
				num=data[i].timesWatched;
			var lastWatched=0;
			if(data[i].lastSelected!=null)
				lastWatched=data[i].lastSelected;
			var reason=data[i].prevalentReason;
			reason=locpop.fixReasonName(reason);
			if(id!=""){
				var prom1=locpop.insert(from_id,id,num,j,lastWatched,reason);
				prom1.then(function(){
					var prom2=insertData(from_id,i+1,j,data);
					prom2.then(function(){
						resolve();
					});
				});
			}else{
				var prom2=insertData(from_id,i+1,j,data);
				prom2.then(function(){
					resolve();
				});
			}
		}else{
			if(debug) console.log("insertData finished!");
			resolve();
		}
	});
	return prom;
	
}
