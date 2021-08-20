package com.flink.demo.cases;

import lombok.SneakyThrows;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

public class ReflectTest {

    @SneakyThrows
    @Test
    public void testReflect() {
        ReflectObj obj = new ReflectObj();
        obj.setName("nameA");
        SubField subField = new SubField("f1", "f2");
        obj.setSubField(subField);

        Class<? extends ReflectObj> personClass = obj.getClass();
        Field[] declaredFields = personClass.getDeclaredFields();
        for (Field field : declaredFields) {
            field.setAccessible(true);
            Object value = field.get(obj);
            Class<?> type = field.getType();
            if (!type.isAnnotationPresent(SettingsContainer.class)) {
                System.out.println(field.getName() + ":" + value);
            } else {
                Field[] typeDeclaredFields = type.getDeclaredFields();
                for (Field typeField : typeDeclaredFields) {
                    typeField.setAccessible(true);
                    String name = typeField.getName();
                    Object o = typeField.get(value);
                    System.out.println(name + "=" + o.toString());
                }
            }
        }
    }


}
