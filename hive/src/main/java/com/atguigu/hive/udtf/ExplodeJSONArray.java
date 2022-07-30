package com.atguigu.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.json.JSONArray;

import java.util.ArrayList;

public class ExplodeJSONArray extends GenericUDTF {
//com.atguigu.hive.udtf.ExplodeJSONArray
    private  PrimitiveObjectInspector inputOI;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        //1. 校验输入参数
        if(argOIs.length!=1){
            throw new UDFArgumentException("ExplodeJSONArray函数只能接收一个参数");
        }

        ObjectInspector arrOI=argOIs[0];

        if(arrOI.getCategory()!=ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentException("ExplodeJSONArray函数只能接收基本数据类型的参数");
        }

        PrimitiveObjectInspector primitiveOI=(PrimitiveObjectInspector) arrOI;
        inputOI=primitiveOI;

        if(primitiveOI.getPrimitiveCategory()!=PrimitiveObjectInspector.PrimitiveCategory.STRING){
            throw new UDFArgumentException("ExplodeJSONArray函数只能接收String数据类型的参数");
        }

        //2. 返回一个对象检查器
        ArrayList<String> fileNames=new ArrayList<String>();
        ArrayList<ObjectInspector> fileOIs=new ArrayList<ObjectInspector>();
        fileNames.add("item");
        fileOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fileNames, fileOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        Object arg=args[0];

        String jsonArrayStr= PrimitiveObjectInspectorUtils.getString(arg,inputOI);

        JSONArray  jsonArray= new JSONArray(jsonArrayStr);

        for (int i = 0; i < jsonArray.length(); i++) {
            String json = jsonArray.getString(i);
            String[] result={json};
            forward(result);
        };



    }

    @Override
    public void close() throws HiveException {

    }
}
