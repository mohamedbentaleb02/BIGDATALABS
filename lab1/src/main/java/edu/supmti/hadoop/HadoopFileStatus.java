package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        FileSystem fs;

        try {
            fs = FileSystem.get(conf);

            // 1. Définir le chemin du fichier sur HDFS
            Path filepath = new Path("/user/root/input/purchases.txt");

            // 2. Vérifier si le fichier existe
            if (!fs.exists(filepath)) {
                System.out.println("File does not exist: " + filepath);
                // System.exit(1); // On commente pour ne pas arrêter le programme
                return; // Quitter si le fichier n'existe pas
            }

            // 3. Obtenir les informations (FileStatus)
            FileStatus status = fs.getFileStatus(filepath);

            // 4. Afficher les informations
            System.out.println("File Name: " + filepath.getName());
            System.out.println("File Size: " + status.getLen() + " bytes");
            System.out.println("File owner: " + status.getOwner());
            System.out.println("File permission: " + status.getPermission());
            System.out.println("File Replication: " + status.getReplication());
            System.out.println("File Block Size: " + status.getBlockSize());

            // 5. Afficher les localisations des blocs
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());

            System.out.println("--- Block Locations ---");
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                String[] hosts = blockLocation.getHosts();
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println("\n-------------------------");
            }

            // 6. Renommer le fichier
            Path newFilepath = new Path("/user/root/input/achats.txt");
            System.out.println("Renaming file to: " + newFilepath);
            fs.rename(filepath, newFilepath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}