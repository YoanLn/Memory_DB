package com.memorydb;

import io.quarkus.runtime.Quarkus;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

/**
 * Classe principale de l'application
 */
@ApplicationPath("/")
public class MemoryDbApplication extends Application {

    /**
     * Point d'entrée principal de l'application
     * @param args Les arguments de la ligne de commande
     */
    public static void main(String[] args) {
        Quarkus.run(args);
    }
} 