package com.atguigu.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @auther ai
 * @date 2020/7/29 14:05
 */
public class NewUTF extends GenericUDF {
    @Description(
            name = "mylen",
            value="FUNC_(string)-Retuers the length of the input string.",
            extended = "Examople:\n"+
            "_FUNC_('abc') will return 3."
    )
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        //检查输入参数类型
        if (objectInspectors.length!=1)throw new UDFArgumentException("Only accept one parameter!");
        if (!objectInspectors[0].getTypeName().equalsIgnoreCase("string")){
            throw new UDFArgumentTypeException(0,"Not a string:"+objectInspectors[0].getTypeName());
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;//返回值类型
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object o=deferredObjects[0].get();
        if (o==null)return 0;
        return o.toString().length();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return getStandardDisplayString("mylen",strings);
    }
}
