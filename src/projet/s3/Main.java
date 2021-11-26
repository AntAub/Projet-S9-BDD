package projet.s3;

import projet.s3.bdd.Databases;
import projet.s3.bdd.MongoDB.MongoDBUtils;

public class Main {

    public static void main(String[] args) {

        Databases dbs = new Databases();

        MongoDBUtils.createIndex(dbs);
        MongoDBUtils.createInvertIndex(dbs);
    }
}
