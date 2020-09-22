package com.flink.demo.cases;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Person {

    private String name;

    @JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
    private boolean nativeColumn = false;

}
