package com.kafka.stream.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Department {

    @JsonProperty("ID")
    private String id;

    @JsonProperty("NAME")
    private String name;
}
