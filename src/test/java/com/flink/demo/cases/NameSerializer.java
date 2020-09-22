package com.flink.demo.cases;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class NameSerializer extends StdSerializer<Name>{

    public NameSerializer() {
        this(null);
    }

    protected NameSerializer(Class<Name> t) {
        super(t);
    }

    @Override
    public void serialize(Name name, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeObject(name.lastNames);
    }
}
