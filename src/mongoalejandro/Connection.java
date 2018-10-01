/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongoalejandro;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

/**
 *
 * @author Extreme PC
 */
public class Connection {

    /**
     * @param args the command line arguments
     */
    private DB database;
    
    public Connection() {
      MongoClient mongoClient = new MongoClient();
      database = mongoClient.getDB("labMongo");
    }
    
    public void insertarPelicula(String id, String nombre,String genero,String director,String franquicia,String pais,String año, String duracion, String productora, String x1, String x2, String x3){
        List<String> actores = Arrays.asList(x1,x2,x3);
        DBObject pelicula = new BasicDBObject("_id", id)
        .append("nombre", nombre)
        .append("genero",genero)
        .append("director", director)
        .append("franquicia", franquicia)
        .append("pais", pais)
        .append("año", año)
        .append("duracion", duracion)
        .append("productora", productora)
        .append("actores", actores);
        DBCollection collection = database.getCollection("Pelicula");
        
        collection.insert(pelicula);
    }
    
    public void insertarProductora(String id, String nombre,String año,String url){
        
        DBObject productoras = new BasicDBObject("_id", id)
        .append("nombre", nombre)
        .append("año", año)
        .append("duracion", url);
        DBCollection collection = database.getCollection("productoras");
        
        collection.insert(productoras);
    }
    
    public void deletePelicula(String id){
        DBCollection collection = database.getCollection("Pelicula");
        BasicDBObject document = new BasicDBObject();
        document.put("_id", id);
        collection.remove(document);
    }
    
    public void deleteProductora(String id){
        DBCollection collection = database.getCollection("productoras");
        BasicDBObject document = new BasicDBObject();
        document.put("_id", id);
        collection.remove(document);
    }
    
    public void updatePelicula(String id, String nombre,String genero,String director,String franquicia,String pais,String año, String duracion, String productora, String x1, String x2, String x3){
        BasicDBObject newDocument = new BasicDBObject();
        List<String> actores = Arrays.asList(x1,x2,x3);
	newDocument.append("$set", new BasicDBObject().append("nombre", nombre)
        .append("genero",genero)
        .append("director", director)
        .append("franquicia", franquicia)
        .append("pais", pais)
        .append("año", año)
        .append("duracion", duracion)
        .append("productora", productora)
        .append("actores", actores)
        );
			
	BasicDBObject searchQuery = new BasicDBObject().append("_id", id);
        DBCollection collection = database.getCollection("Pelicula");
	collection.update(searchQuery, newDocument);
    }
    
    public void updateProductora(String id, String nombre,String año,String url){
        BasicDBObject newDocument = new BasicDBObject();
	newDocument.append("$set", new BasicDBObject()
        .append("nombre", nombre)
        .append("año", año)
        .append("duracion", url));
        
        BasicDBObject searchQuery = new BasicDBObject().append("_id", id);
        DBCollection collection = database.getCollection("productoras");
        collection.update(searchQuery, newDocument);
        
    }
   
    public Vector<String> consulta1(String nom){
        BasicDBObject query = new BasicDBObject().append("nombre", nom); // WHERE name = "Jon"
       DBCollection collection = database.getCollection("Pelicula");
       DBCursor cursor = collection.find(query); // FROM yourCollection
       Vector x = new Vector();
       x.add(cursor.next());
       return x;
    }
    
    public Vector<String> consulta2(String fran){
       BasicDBObject query = new BasicDBObject().append("franquicia", fran); // WHERE name = "Jon"
       DBCollection collection = database.getCollection("Pelicula");
       DBCursor cursor = collection.find(query); // FROM yourCollection
       Vector x = new Vector();
       try {
       while(cursor.hasNext()) {
            x.add(cursor.next());
        }
            } finally {
               cursor.close();
            }
       return x;
    }
    
    public Vector<String> consulta3(String pro){
       BasicDBObject fields = new BasicDBObject().append("nombre", 1)
                                                 .append("genero", 2)
                                                 .append("año", 3); // SELECT name
       
       BasicDBObject query = new BasicDBObject().append("productora", pro); // WHERE name = "Jon"
       DBCollection collection = database.getCollection("Pelicula");
       DBCursor cursor = collection.find(query, fields); // FROM yourCollection
       Vector x = new Vector();
       try {
        while(cursor.hasNext()) {
            x.add(cursor.next());
        }
            } finally {
               cursor.close();
            }
       return x;
    }
    
    
    
}
