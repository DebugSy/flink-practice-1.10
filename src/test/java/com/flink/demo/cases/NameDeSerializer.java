package com.flink.demo.cases;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.List;

public class NameDeSerializer extends StdDeserializer<Name> {

    public NameDeSerializer() {
        this(null);
    }

    @Override
    public Name deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

        TypeReference<List<SubField>> typeRef = new TypeReference<List<SubField>>() {};
        List<SubField> list = jsonParser.getCodec().readValue(jsonParser, typeRef);
        Name name = new Name();
        name.setLastNames(list);
        return name;
    }

    protected NameDeSerializer(Class<Name> t) {
        super(t);
    }


}
