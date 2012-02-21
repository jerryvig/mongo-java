import com.mongodb.*
import java.lang.Math

def m = new Mongo("localhost")
def db = m.getDB("morningstar")

def dividendsColl = db.getCollection("yahoo_dividends")
def eodColl = db.getCollection("yahoo_eod")
def dividendsTickerList = dividendsColl.distinct("ticker_symbol")
def eodTickerList = eodColl.distinct("ticker_symbol")

def combinedList = []
dividendsTickerList.each { 
  combinedList.add( it )
}
eodTickerList.each {
  combinedList.add( it )
}
def distinctList = combinedList.unique()

db.getCollection("ticker_data").drop()
def tickerColl = db.createCollection("ticker_data",new BasicDBObject())

distinctList.each {
  def outDBObject = new BasicDBObject()
  outDBObject.put("ticker_symbol",it)
  def dividendList = new BasicDBList()
  def eodList = new BasicDBList()

  def query = new BasicDBObject()
  query.put("ticker_symbol",it)

  def sorter = new BasicDBObject()
  sorter.put("pay_date",1)
  def cur = dividendsColl.find(query).sort( sorter )

  def count = 0
  def sumDividends = 0.0

  while ( cur.hasNext() ) {
    def nextDoc = cur.next()
    def divObj = new BasicDBObject()
    divObj.put("pay_date",nextDoc.get("pay_date"))
    divObj.put("dividend",nextDoc.get("dividend"))
    if ( nextDoc.get("pay_date") >= "2011-02-17" ) {
       sumDividends += nextDoc.get("dividend")       
    }
    dividendList.put(count,divObj)
    count++
  } 
  outDBObject.put("dividends",dividendList)

  sorter = new BasicDBObject()
  sorter.put("eod_date",1)
  def cur2 = eodColl.find(query).sort(sorter)

  count = 0
  def sharpeCount = 0
  def lastClose = 0.0
  def prevAdjClose = 0.0
  def sumReturnsSquared = 0.0
  def sumReturns = 0.0

  while ( cur2.hasNext() ) {   
    def nextDoc = cur2.next()
    def eodObj = new BasicDBObject()
    eodObj.put("eod_date",nextDoc.get("eod_date"))
    eodObj.put("open",nextDoc.get("open"))
    eodObj.put("high",nextDoc.get("high"))
    eodObj.put("low",nextDoc.get("low"))
    eodObj.put("close",nextDoc.get("close"))
    eodObj.put("volume",nextDoc.get("volume"))
    eodObj.put("adj_close",nextDoc.get("adj_close"))

    if ( nextDoc.get("eod_date") > "2011-09-01" ) {
     if ( sharpeCount > 0 ) {
      def dailyChange = (nextDoc.get("adj_close")-prevAdjClose)/prevAdjClose
      eodObj.put("daily_change",dailyChange)
      sumReturns += dailyChange
      sumReturnsSquared += (dailyChange*dailyChange)
     }
     sharpeCount++
    }

    eodList.put(count,eodObj)
    prevAdjClose = nextDoc.get("adj_close")
    count++

    if ( nextDoc.get("eod_date").equals("2012-02-17") ) {
       lastClose = nextDoc.get("close")
    }
  }
  outDBObject.put("eod_data",eodList)
  outDBObject.put("trailing_annual_dividend",sumDividends.doubleValue())

  if ( count > 0 ) {
   def avgReturn = sumReturns/sharpeCount
   def stddevReturns = Math.sqrt( sumReturnsSquared/sharpeCount - Math.pow(avgReturn,2.0) )
   def sharpeRatio = avgReturn/stddevReturns
   outDBObject.put("sharpe_ratio",sharpeRatio)
 }

  if ( lastClose > 0.0 ) {
   def dividendYield = sumDividends/lastClose
   outDBObject.put("dividend_yield",dividendYield)
  }

  tickerColl.insert( outDBObject )
}
