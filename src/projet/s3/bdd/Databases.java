package projet.s3.bdd;

import projet.s3.bdd.MongoDB.MongoDB;
import projet.s3.bdd.Neo4J.Neo4J;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Databases {

    private MongoDB mongoDB;
    private Neo4J neo4J;

    public Databases(){
        /* On retire les messages d'information du drive MongoDB */
        Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
        mongoLogger.setLevel(Level.WARNING);

        this.establishMongoDBConnection();
        this.establishNeo4JConnection();

        System.out.println("-----------------------------------------------------------");
    }

    /**
     * Etablissement de la connexion à la base de données MongoDB
     */
    private void establishMongoDBConnection(){

        try {
            /* Connexion à la base de données MongoDB */
            this.mongoDB = new MongoDB(Config.mongoDBhostAddress, Config.mongoDBhostport);

            /* Choix de la base de données */
            //System.out.println("Databases disponibles : " + mongoDB.listDatabases().toString());
            mongoDB.setDatabase(Config.mongoDBdbName);

            /* Choix de la collection */
            //System.out.println("Collections disponibles : " + mongoDB.listCollection().toString());
            //mongoDB.setCollection("index");
        }
        catch (Exception e){

            System.err.println(e.getMessage());
        }
    }

    /**
     * Etablissement de la connexion à la base de données Neo4J
     */
    private void establishNeo4JConnection(){
        try{
            /* Connexion à la base de données Neo4J */
            this.neo4J = new Neo4J(Config.neo4JhostAddress,Config.neo4Jhostport);
        }
        catch (Exception e){

            System.err.println(e.getMessage());
        }
    }

    /**
     * Récupere la
     * @return Objet de type MongoDB pour manipulation de la base de données MongoDB
     */
    public MongoDB getMongoDB() {
        return mongoDB;
    }

    public Neo4J getNeo4J() {
        return neo4J;
    }
}
