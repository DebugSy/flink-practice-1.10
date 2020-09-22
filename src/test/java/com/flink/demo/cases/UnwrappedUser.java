package com.flink.demo.cases;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class UnwrappedUser {

    public Name name;

    public Name name1;

    public UnwrappedUser(int id, Name name) {
        this.name = name;
        this.name1 = name;
    }
}
