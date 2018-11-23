package areapatition;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class AreaPaetitioner<KEY, Value> extends Partitioner<KEY, Value> {


    private static HashMap<String, Integer> areaMap = new HashMap<String, Integer>();


    static {
        areaMap.put("135", 0);
        areaMap.put("136", 1);
        areaMap.put("137", 2);
        areaMap.put("138", 3);
        areaMap.put("139", 4);
    }


    @Override
    public int getPartition(KEY key, Value value, int i) {
        // 不能在这里查询数据库。这里会多次访问数据库。
        int areaCode = areaMap.get(key.toString().substring(0, 3)) == null ? 5 : areaMap.get(key.toString().substring(0, 3));

        return areaCode;
    }
}
