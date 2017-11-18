package org.apache.phoenix;

import org.apache.hadoop.hbase.util.MD5Hash;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by fengshen on 11/17/17.
 */
public class GenHash {
    public static void main(String[] args) {

        System.out.printf( "| %1$8d | %2$8s | %3$4s |\n", 6, "corejava", null );
        System.out.printf( "| %1$8d | %2$8s | %3$4s |", 612222, "1", null );

    }

    public static ArrayList<String> genHash(int regions) {
        ArrayList<String> re = new ArrayList<String>();

        Map<String, Integer> sss = new TreeMap<String, Integer>();

        int count = 1000000;
        for (int i = 0; i < count; i++) {
            String key = MD5Hash.getMD5AsHex(String.valueOf(i).getBytes()).substring(0, 5);
            if (!sss.containsKey(key)) {
                sss.put(key, 1);
            } else {
                sss.put(key, sss.get(key) + 1);
            }
        }
        System.out.println(sss.size());

        int setup = count / regions;

        Set<Map.Entry<String, Integer>> it = sss.entrySet();
        Iterator<Map.Entry<String, Integer>> ok = it.iterator();
        int i = 0;
        int start = 0;
        Map.Entry<String, Integer> v = null;
        while (ok.hasNext()) {
            if (i >= (start + 1) * setup) {
                re.add(v.getKey());
                start++;
            }
            v = ok.next();
            i = i + v.getValue();

        }

        return re;
    }
}
