import com.mongodb.*;
import java.lang.Math;
import java.util.NoSuchElementException;
import au.com.bytecode.opencsv.CSVWriter;

def mongo = new Mongo("localhost");
def db = mongo.getDB("morningstar");

CSVWriter outWriter = new CSVWriter( new FileWriter("/tmp/morningstar_rev_fact_table.csv") );

def collection = db.getCollection("morningstar_revenue");
def geoColl = db.getCollection("geocode_addresses");
def yahooColl = db.getCollection("yahoo_profile_info");

def tickers = collection.distinct("ticker_symbol");
tickers.each {
 // println it;
  def query = new BasicDBObject("ticker_symbol",it);
  def cursor = collection.find( query ).sort( new BasicDBObject("period",1) );


  def tickerSymbol = it.trim();
  def rev2006 = 0;
  def rev2007 = 0;
  def rev2008 = 0;
  def rev2009 = 0;
  def rev2010 = 0;
  def revTTM = 0;
  def revGrowth2007 = 0.0;
  def revGrowth2008 = 0.0;
  def revGrowth2009 = 0.0;
  def revGrowth2010 = 0.0;
  def revGrowthTTM = 0.0;
  
  cursor.each { cur ->
    def period = cur.get("period");
    if ( period.startsWith("2006") ) {
       rev2006 = cur.get("revenue");
    }
    else if ( period.startsWith("2007") ) {
       rev2007 = cur.get("revenue");
    }
    else if ( period.startsWith("2008") ) {
       rev2008 = cur.get("revenue");
    }
    else if ( period.startsWith("2009") ) {
       rev2009 = cur.get("revenue");
    }
    else if ( period.startsWith("2010") ) {
       rev2010 = cur.get("revenue");
    }
    else if ( period.startsWith("TTM") ) {
       revTTM = cur.get("revenue");
    }
  }

  def countChanges = 0;
  def sumRevGrowth = 0.0;
  def sumReGrowthSquared = 0.0;

  if ( rev2006 > 0.0 ) {
    revGrowth2007 = (rev2007-rev2006)/rev2006;
    sumRevGrowth += revGrowth2007;
    sumRevGrowthSquared = revGrowth2007*revGrowth2007;
    countChanges++; 
  }
  if ( rev2007 > 0.0 ) {
    revGrowth2008 = (rev2008-rev2007)/rev2007;
    sumRevGrowth += revGrowth2008;
    sumRevGrowthSquared = revGrowth2008*revGrowth2008;
    countChanges++; 
  }
  if ( rev2008 > 0.0 ) {
    revGrowth2009 = (rev2009-rev2008)/rev2008;
    sumRevGrowth += revGrowth2009;
    sumRevGrowthSquared = revGrowth2009*revGrowth2009;
    countChanges++;   
  } 
  if ( rev2009 > 0.0 ) {
    revGrowth2010 = (rev2010-rev2009)/rev2009;
    sumRevGrowth += revGrowth2010;
    sumRevGrowthSquared = revGrowth2010*revGrowth2010;
    countChanges++; 
  } 
  if ( rev2010 > 0.0 ) {
    revGrowthTTM = (revTTM-rev2010)/rev2010;
    sumRevGrowth += revGrowthTTM;
    sumRevGrowthSquared = revGrowthTTM*revGrowthTTM;
    countChanges++;   
  }
  
  def avgRevGrowth = 0.0;
  def sharpeRevGrowth = 0.0;

  if ( countChanges > 0 ) {
    avgRevGrowth = sumRevGrowth/countChanges;
    avgRevGrowthSquared = sumRevGrowthSquared/countChanges;
    def stdDev = Math.sqrt(avgRevGrowthSquared - avgRevGrowth*avgRevGrowth);
    sharpeRevGrowth = avgRevGrowth/stdDev; 
  }

  def geoCur = geoColl.find( query );
  def state = "";
  def city = "";
  def streetAddress = "";
  def zipCode = ""; 
  def companyName = "";
  def phone = "";
  def fax = "";
  def website = "";
  def indexMembership = "";
  def sector = "";
  def industry = "";
  def fullTimeEmp = "";

  try {
    def obj = geoCur.next();
    streetAddress = obj.get("street_address");
    city = obj.get("city");
    state = obj.get("state");
    zipCode = obj.get("zip_code");  
  } catch ( NoSuchElementException nsee ) { }
   
  def yahooCur = yahooColl.find( query );
  try {
    def obj = yahooCur.next();
    companyName = obj.get("company_name");
    phone = obj.get("phone");
    fax = obj.get("fax");
    website = obj.get("website");
    indexMembership = obj.get("index_membership");
    sector = obj.get("sector");
    industry = obj.get("industry");
    fullTimeEmployees = obj.get("full_time_employees");
  } catch ( NoSuchElementException nsee ) { }
 
  String[] nextLine = new String[27];
  nextLine[0] = tickerSymbol;
  nextLine[1] = rev2006;
  nextLine[2] = rev2007;
  nextLine[3] = rev2008;
  nextLine[4] = rev2009;
  nextLine[5] = rev2010;
  nextLine[6] = revTTM;
  nextLine[7] = revGrowth2007;
  nextLine[8] = revGrowth2008;
  nextLine[9] = revGrowth2009;
  nextLine[10] = revGrowth2010;
  nextLine[11] = revGrowthTTM;
  nextLine[12] = countChanges;
  nextLine[13] = avgRevGrowth;
  nextLine[14] = sharpeRevGrowth;
  nextLine[15] = streetAddress;
  nextLine[16] = city;
  nextLine[17] = state;
  nextLine[18] = zipCode;
  nextLine[19] = companyName;
  nextLine[20] = phone;
  nextLine[21] = fax;
  nextLine[22] = website;
  nextLine[23] = indexMembership;
  nextLine[24] = sector;
  nextLine[25] = industry;
  nextLine[26] = fullTimeEmp;
  
  outWriter.writeNext( nextLine );
}

outWriter.close();
