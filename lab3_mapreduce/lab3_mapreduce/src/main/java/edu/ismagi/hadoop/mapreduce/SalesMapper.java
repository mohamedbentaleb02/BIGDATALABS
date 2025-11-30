package edu.ismagi.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text category = new Text();
    private DoubleWritable price = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // On découpe la ligne CSV par les virgules
        String[] fields = line.split(",");

        // Vérification basique pour éviter les lignes vides ou mal formées
        if (fields.length > 4) {
            String productCategory = fields[3]; // L'index 3 est la catégorie
            String productPrice = fields[4];    // L'index 4 est le prix

            try {
                category.set(productCategory);
                price.set(Double.parseDouble(productPrice));
                context.write(category, price);
            } catch (NumberFormatException e) {
                // On ignore les lignes où le prix n'est pas un nombre valide
            }
        }
    }
}