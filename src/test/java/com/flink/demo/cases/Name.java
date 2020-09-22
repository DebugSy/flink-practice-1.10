package com.flink.demo.cases;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
//@JsonSerialize(using = NameSerializer.class)
//@JsonDeserialize(using = NameDeSerializer.class)
public class Name {

    public String firstName;

    @JsonValue
    public List<SubField> lastNames;

    @JsonCreator
    public Name(List<SubField> lastNames) {
        this.lastNames = lastNames;
    }

    public Name(String firstName, List<SubField> lastNames) {
        this.firstName = firstName;
        this.lastNames = lastNames;
    }
}
