importPackage(com.mongodb);

var m = new Mongo("localhost");
var db = m.getDB("morningstar");
var cboeWeekliesColl = db.getCollection("cboe_weeklies_eod");
var cboeWeekliesList = cboeWeekliesColl.distinct("ticker_symbol");

db.getCollection("cboe_ticker_data").drop();
var cboeTickerColl = db.createCollection("cboe_ticker_data",new BasicDBObject());

for ( var i=0; i<cboeWeekliesList.size(); i++ ) {
  var outDBObject = new BasicDBObject();
  outDBObject.put("ticker_symbol",cboeWeekliesList.get(i));
  var eodList = new BasicDBList();
  
  var query = new BasicDBObject();
  query.put("ticker_symbol",cboeWeekliesList.get(i));
  var sorter = new BasicDBObject();
  sorter.put("eod_date",1);

  var cur = cboeWeekliesColl.find(query).sort(sorter);
  var listCount = 0;
  var sharpeCount = 0;
  var prevAdjClose = 0.0;
  var sumChangeSquared = 0.0;
  var sumChange = 0.0;

  while ( cur.hasNext() ) {
    var row = cur.next();
    var eodObj = new BasicDBObject();
    eodObj.put("eod_date",row.get("eod_date"));
    eodObj.put("open",row.get("open"));
    eodObj.put("high",row.get("high"));
    eodObj.put("low",row.get("low"));
    eodObj.put("close",row.get("close"));
    eodObj.put("volume",row.get("volume"));
    eodObj.put("adj_close",row.get("adj_close"));
    eodList.put(listCount,eodObj);
    listCount++;

    if ( row.get("eod_date") > "2011-01-01" ) {
      if ( sharpeCount > 0 ) {
	var dayChange = (row.get("adj_close")-prevAdjClose)/prevAdjClose;
        eodObj.put("day_change",dayChange);
        sumChange += dayChange;
        sumChangeSquared += dayChange*dayChange;
      }       
      sharpeCount++;
    }
    prevAdjClose = row.get("adj_close");
  }
  outDBObject.put("eod_data",eodList);

  if ( sharpeCount > 0 ) {
     var avgChange = sumChange/sharpeCount;
     var stddevChange = Math.sqrt( sumChangeSquared/sharpeCount - avgChange*avgChange );
     var sharpeRatio = avgChange/stddevChange;
     outDBObject.put("sharpe_ratio",sharpeRatio); 
  }

  cboeTickerColl.insert( outDBObject ); 
}
