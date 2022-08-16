package com.kafka.stream.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class EmployeeHistory {
    @JsonProperty("ID")
    private String id;

    @JsonProperty("EMPLOYER_NAME")
    private String employerName;

    @JsonProperty("EMPLOYEE_ID")
    private String employeeId;
    
}
