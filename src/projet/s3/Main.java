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
            // TODO Refaire la fonction
            /* Car c'est pas la bonne methode (cf. sujet) mais il faut pas importer toute la puis faire le traitement.
            Il faut recupérée element par element et utiliser replaceOne pour actuliser.
            --> .replaceOne(Filters.eq("_id", docExistant.get("_id")),docExistant
            C'est logique car on peut pas se permettre de télécharger toute la base en local si elle fait 10Go
            */
            // TODO Trouver pourquoi on à 2845 et pas 2846 comme annoncé dans le sujet
            worker.createInvertIndex();

            /* 3.4. Recherche de documents */
            worker.searchInvertIndex("with");

            /* 3.5. Auteurs ayant écrit le plus d’articles */
            worker.get10AuthorsWithMostArticles();

            /* 3.6. Recherche de documents avancée */
            worker.searchInvertIndexAdvanced(new ArrayList<>() {{
                add("with");
                add("systems");
                add("new");
            }});

            /* Nettoyage de la base MongoDB */
            worker.clear();

            /* Fermeture des connexions */
            worker.closeConnections();

        }
        catch (Exception e){
            e.printStackTrace();
            System.err.println("Erreur lors de l'execution : "  + e.getMessage());
        }
    }
}

