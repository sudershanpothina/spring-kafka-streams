package com.kafka.stream.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.ArrayList;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Employee {
    @JsonProperty("ID")
    private String id;

    @JsonProperty("NAME")
    private String name;

    @JsonProperty("DEPARTMENT_ID")
    private String departmentId;

    @JsonProperty("DEPARTMENT")
    private Department department;

    @JsonProperty("EMPLOYEE_HISTORY")
    private ArrayList<EmployeeHistory> employeeHistories;
}
