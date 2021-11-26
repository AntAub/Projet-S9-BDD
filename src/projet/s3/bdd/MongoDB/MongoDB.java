package projet.s3.bdd.MongoDB;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;

/**
 * Classe d'utilisateur d'une base de donnée MongoDB
 */
public class MongoDB {

    private final String hostName;
    private MongoClient client;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    /**
     * Constructeur
     * @param host Adresse du serveur MongoDB
     * @param port Port d'écoute du serveur MongoDB
     * @throws Exception Erreur de connexion à la base de donnée MongoDB
     */
    public MongoDB(String host, int port) throws Exception {

        this.hostName = host;

        System.out.println("Tentative de connexion à " + this.hostName + " ...");
        MongoClientURI connectionString = new MongoClientURI("mongodb://" + host + ":" + port);

        try {

            this.client= new MongoClient(connectionString);
            this.client.getAddress();
            System.out.println("Connexion établie à MongoDB " + this.hostName);
        } catch (Exception e) {

            this.client.close();
            throw new Exception("Connexion impossible à " + this.hostName + "\n" + e.getMessage());
        }
    }

    /**
     * Affiche la liste des base de données disponibles dans MongoDB
     * @return Liste des base de données disponibles dans MongoDB
     */
    public ArrayList<String> listDatabases(){

        ArrayList<String> databases = new ArrayList<>();
        for (String databaseName : client.listDatabaseNames())
            databases.add(databaseName);
        return databases;
    }

    /**
     * Choix de la base de données à utiliser dans MongoDB
     * @param databaseName Nom de la base de données
     */
    public void setDatabase(String databaseName){

        try{
            this.database = client.getDatabase(databaseName);
            System.out.println("Base de données séléctionnée : " + this.database.getName() + " dans " + this.hostName);
        }
        catch (Exception e){
            System.err.println("La base de données demandée n'exite pas. Merci de choisir parmis la liste suivante");
        }
    }

    /**
     * Affiche la liste des collection disponible dans base de données séléctionnée
     * @return Liste des collection disponible dans base de données séléctionnée
     */
    public ArrayList<String> listCollection(){

        ArrayList<String> collections = new ArrayList<>();
        for (String collection : this.database.listCollectionNames())
            collections.add(collection);
        return collections;
    }

    /**
     * Choix de la collection à utiliser dans MongoDB
     * @param collectionName Nom de la collection
     */
    public void setCollection(String collectionName){

        try{
            this.collection = this.database.getCollection(collectionName);
            System.out.println("Collection séléctionnée : " + this.collection.getNamespace());
        }
        catch (Exception e){
            System.err.println("La collection demandée n'exite pas. Merci de choisir parmis la liste suivante");
        }
    }

    /**
     * Retourne la collection séléctionnée
     * @return MongoCollection<Document> de la collection séléctionnée
     */
    public MongoCollection<Document> getCollection() {
        return collection;
    }

    /**
     * Liste les documents d'une collection
     * @return liste des documents d'une collection
     */
    public FindIterable<Document> listerDocuments(){
        return this.collection.find();
    }
}
