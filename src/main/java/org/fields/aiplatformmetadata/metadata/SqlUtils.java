package org.fields.aiplatformmetadata.metadata;

import java.util.Map;

public class SqlUtils {
    public static String getSql(String tableName, String operation, Map<?, ?> datas){
        String condition = null;
        if(operation.equals("select")){
            condition = " a.* from  " + tableName + " a where ";
        }else if(operation.equals("update")){
            condition = " " + tableName + " a set ";
        }else if(operation.equals("delete")){
            condition = " from " + tableName + " a where ";
        }else if(operation.equals("insert")){
            condition = " into " + tableName + " values (";
            String link = "";
            for(Map.Entry<?, ?> entry: datas.entrySet()){
                condition = condition + link + entry.getKey();
                link = ",";
            }
            condition = condition + ") values (";
        }
        String value = "",link = "";
    }
}
