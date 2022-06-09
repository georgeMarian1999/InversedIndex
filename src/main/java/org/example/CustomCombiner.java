package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// Combiner face acelasi lucru ca si Reducer doar ca pe un set mai mic de date
public class CustomCombiner extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // vom avea un map de perechi (cuv, multime de nrLine)
        Set<String> result = new HashSet<String>();
        HashMap<String, Set<String>> model = new HashMap<String, Set<String>>();
        // luam fiecare valoare care este de fapt numeFis~nrLinie
        for (Text val : values) {
            String[] parts = val.toString().split("-");
            // verificam daca am mai avut numele curent de fisier, daca nu adaugam un nou
            // element in dictionar, daca il mai gasim, adugam doar nrLine la lista asociata
            if (model.containsKey(parts[0]))
                model.get(parts[0]).add(parts[1]);
            else {
                Set<String> dictionaryValue = new HashSet<String>();
                dictionaryValue.add(parts[1]);
                model.put(parts[0], dictionaryValue);
            }
        }
        // formam rezultatul pe care il trimitem mai departe
        for (Map.Entry<String, Set<String>> entry : model.entrySet()) {
            String mapKey = entry.getKey();
            Set<String> mapValue = entry.getValue();
            result.add(mapKey + " (" + join(mapValue, ",") + ")");
        }
        String finalCombinerResult = join(result, "#");
        context.write(key, new Text(finalCombinerResult));
    }

    // Functia primeste o lista si un separator si concateaza toate valorile
    // din lista folosind acel separator
    public static String join(Set<String> list, String separator) {
        String result = "";
        for (String s : list) {
            result += s + separator;
        }
        result = result.substring(0, result.length() - 1);
        return result;
    }
}