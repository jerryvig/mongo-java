import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.List;

public class MongoTest {
  public static void main( String[] args ) {
    try {
      Mongo m = new Mongo("localhost");   
      DB db = m.getDB("mydb");

      DBCollection collection = db.getCollection("testCollection");
      collection.drop();
      

      BasicDBObject doc = new BasicDBObject();
      doc.put( "name", "MongoDB" );
      doc.put( "type", "database" );
      doc.put( "count", 1 );

      BasicDBObject info = new BasicDBObject();
      info.put("x", 203);
      info.put("y", 102);
      
      doc.put("info", info);

      collection.insert( doc );
      DBObject myDoc = collection.findOne();
      System.out.println( myDoc );

      for ( int i=0; i<100; i++ ) {
	  collection.insert( new BasicDBObject().append("i", i));
      }

      System.out.println( collection.getCount() ); 

      BasicDBObject query = new BasicDBObject();
      query.put("i", new BasicDBObject("$gt", 50));
      DBCursor cur = collection.find(query);

      while ( cur.hasNext() ) {
	  System.out.println( cur.next() );
      }

      collection.createIndex(new BasicDBObject("i",1));
      List<DBObject> list = collection.getIndexInfo();

      for ( DBObject o : list ) {
	  System.out.println(o);
      }

      for ( String str : m.getDatabaseNames() ) {
	  System.out.println( str );
      }

    } catch ( UnknownHostException uhe ) { uhe.printStackTrace(); }
  }  
}