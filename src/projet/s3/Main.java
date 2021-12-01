package projet.s3;

import projet.s3.bdd.Databases;
import projet.s3.bdd.Worker;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        try {
            /* Création des connexions */
            Worker worker = new Worker();

            /* 3.1. Mettre en place un datastore MongoDB */
            worker.createIndex();

            /* 3.2. Mettre en place un index sur le tableau de motsClés dans MongoDB */
            worker.ensureKeywordsIndex();

            /* 3.3. Mise en place d’une « structure miroir » sur MongoDB */
            // TODO Trouver pourquoi on à 2845 et pas 2846 comme annoncé dans le sujet
            worker.createInvertIndex();

            /* 3.4. Recherche de documents */
            worker.searchInvertIndex("with");
            // TODO Resultat 01 en double : pourquoi ?

            /* 3.5. Auteurs ayant écrit le plus d’articles */
            worker.get10AuthorsWithMostArticles();

            /* 3.6. Recherche de documents avancée */
            //TODO Vérifier que c'est la bonne réponse attendue
            worker.searchInvertIndexAdvanced(new ArrayList<>() {{
                add("with");
                add("systems");
                add("new");
            }});

            /* Fermeture des connexions */
            worker.closeConnections();

        }
        catch (Exception e){
            e.printStackTrace();
            System.err.println("Erreur lors de l'execution : "  + e.getMessage());
        }
    }
}

