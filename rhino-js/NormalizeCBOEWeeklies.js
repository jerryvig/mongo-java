importPackage(com.mongodb);

var m = new Mongo("localhost");
var db = m.getDB("morningstar");
var cboeWeekliesColl = db.getCollection("cboe_weeklies_eod");
var cboeRevColl = db.getCollection("cboe_weeklies_revenues");
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

  var sharpeRatio = 0.0;
  if ( sharpeCount > 0 ) {
     var avgChange = sumChange/(sharpeCount-1);
     var stddevChange = Math.sqrt( sumChangeSquared/(sharpeCount-1) - avgChange*avgChange );
     sharpeRatio = avgChange/stddevChange;
     outDBObject.put("sharpe_ratio",sharpeRatio); 
  }

  sorter = new BasicDBObject();
  sorter.put("period",1);
  var revList = new BasicDBList();

  var cur2 = cboeRevColl.find(query).sort(sorter);
  var revCount = 0;
  var prevRev = 0.0;
  var sumRevGrowth = 0.0;
  var sumRevGrowthSquared = 0.0;

  while ( cur2.hasNext() ) {
     var row = cur2.next();
     var revObj = new BasicDBObject();
     revObj.put("period",row.get("period"));
     revObj.put("revenue",row.get("revenue"));
     revList.put(revCount,revObj);
     if ( revCount > 0 ) {
	var revGrowth = (row.get("revenue")-prevRev)/prevRev;
        sumRevGrowth += revGrowth;
        sumRevGrowthSquared += revGrowth*revGrowth;
     }
     prevRev = row.get("revenue");
     revCount++;   
  }
  outDBObject.put("revenue_data",revList);

  var sharpeRevGrowth = 0.0;
  if ( revCount > 0 ) {
     var avgRevGrowth = sumRevGrowth/(revCount-1);
     var stddevRevGrowth = Math.sqrt( sumRevGrowthSquared/(revCount-1) - avgRevGrowth*avgRevGrowth );
     var sharpeRevGrowth = avgRevGrowth/stddevRevGrowth;
     outDBObject.put("avg_rev_growth",avgRevGrowth);
     outDBObject.put("sharpe_rev_growth",sharpeRevGrowth);
  }

  /*
  if ( sharpeRatio > 0.0 || sharpeRevGrowth > 0.0 ) {
    outDBObject.put("hypotenuse",Math.sqrt( 9.0*sharpeRatio*sharpeRatio + sharpeRevGrowth*sharpeRevGrowth ));
    } */

  cboeTickerColl.insert( outDBObject ); 
}
