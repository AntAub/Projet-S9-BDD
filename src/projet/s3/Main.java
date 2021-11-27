package projet.s3;

import projet.s3.bdd.Databases;
import projet.s3.bdd.MongoDB.MongoDBUtils;
import projet.s3.bdd.Neo4J.Neo4JUtils;

public class Main {

    public static void main(String[] args) {

        Databases dbs = new Databases();


        MongoDBUtils.createIndex(dbs);

        MongoDBUtils.createInvertIndex(dbs);

        MongoDBUtils.ensureIndex(dbs, "{motsCles : 1}");

        Neo4JUtils.close(dbs);
        MongoDBUtils.close(dbs);

    }
}
