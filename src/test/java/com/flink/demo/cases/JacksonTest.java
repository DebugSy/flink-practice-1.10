package com.flink.demo.cases;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class JacksonTest {

    @Test
    public void test() throws IOException {

        ArrayList<SubField> subFields = new ArrayList<>();
        subFields.add(new SubField("n1", "s1"));
        subFields.add(new SubField("n2", "s2"));
        Name name = new Name("John", subFields);
        UnwrappedUser user = new UnwrappedUser(1, name);

        String result = new ObjectMapper().writeValueAsString(user);
        System.out.println(result);

        UnwrappedUser unwrappedUser = new ObjectMapper().readValue(result, UnwrappedUser.class);
        System.out.println(unwrappedUser);

        String result1 = new ObjectMapper().writeValueAsString(unwrappedUser);
        System.out.println(result1);
    }


    @Test
    public void te() {

        ArrayList<String> strings = null;
        Map<String, List<String>> map = new HashMap<>();
        map.put("first", strings);

        ArrayList<String> objects = new ArrayList<>();
        objects.add("11");
        map.put("first", objects);

        System.out.println(map);
    }

    @Test
    public void t2() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        Person person1 = new Person();
        person1.setName("person_1");
        person1.setNativeColumn(true);
        String toJson1 = objectMapper.writeValueAsString(person1);
        System.out.println(toJson1);

        Person person2 = new Person();
        person2.setName("person_2");
        String toJson2 = objectMapper.writeValueAsString(person2);
        System.out.println(toJson2);
    }




}
