package com.kafka.stream;

import com.kafka.stream.model.Department;
import com.kafka.stream.model.Employee;
import com.kafka.stream.model.EmployeeHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@Slf4j
public class Processor {
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final JsonSerde<Department> DEPARTMENT_JSON_SERDE = new JsonSerde<>(Department.class);

    private static final JsonSerde<Employee> EMPLOYEE_JSON_SERDE = new JsonSerde<>(Employee.class);

    private static final JsonSerde<EmployeeHistory> EMPLOYEE_HISTORY_JSON_SERDE = new JsonSerde<>(EmployeeHistory.class);

    @Autowired
    void firstPipeline(StreamsBuilder streamsBuilder) {
        final String EMPLOYEE_TOPIC = "employee";
        final String DEPARTMENT_TOPIC = "department";
        final String EMPLOYMENT_HISTORY_TOPIC = "employment.history";
        final String EMPLOYMENT_MERGE_TOPIC = "employment.merge1";

//        String id = String.valueOf(System.currentTimeMillis());
        Random rand = new Random();
        int randomNum = rand.nextInt((10 - 1) + 1) + 1;
        String id = String.valueOf(randomNum);
        String deptId = "D-" + id;
        String empId = "E-" + id;
        String empHistId = "EH-" + id;


        Department department = new Department();
        department.setId(deptId);
        department.setName("Department " + id);

        Employee employee = new Employee();
        employee.setId(empId);
        employee.setName("Employee " + id);
        employee.setDepartmentId(department.getId());

        KStream<String, String> messageStream = streamsBuilder
                .stream("test", Consumed.with(STRING_SERDE, STRING_SERDE));

        messageStream
                .peek((key, value) -> log.info(value))
                .filter((key, value) -> value.contains("dept"))
                .peek((key, value) -> log.info("sending to dept topic"))
                .map((key, value) -> new KeyValue<>(department.getId(), department))
                .peek((key, value) -> log.info(value.toString()))
                .to(DEPARTMENT_TOPIC, Produced.with(STRING_SERDE, DEPARTMENT_JSON_SERDE));

        messageStream
                .peek((key, value) -> log.info(value))
                .filter((key, value) -> value.contains("emp"))
                .peek((key, value) -> log.info("sending to emp topic"))
                .map((key, value) -> new KeyValue<>(employee.getId(), employee))
                .peek((key, value) -> log.info(value.toString()))
                .to(EMPLOYEE_TOPIC, Produced.with(STRING_SERDE, EMPLOYEE_JSON_SERDE));

        messageStream
                .peek((key, value) -> log.info(value))
                .filter((key, value) -> value.contains("emphist"))
                .peek((key, value) -> log.info("sending to emp history topic"))
                .flatMap((key, value) -> getKeyValues(randomNum, empHistId, employee))
                .to(EMPLOYMENT_HISTORY_TOPIC, Produced.with(STRING_SERDE, EMPLOYEE_HISTORY_JSON_SERDE));

        KTable<String, Department> departmentKTable = streamsBuilder
                .table(DEPARTMENT_TOPIC, Materialized.with(STRING_SERDE, DEPARTMENT_JSON_SERDE));

        KStream<String, Employee> employeeKStream = streamsBuilder
                .stream(EMPLOYEE_TOPIC, Consumed.with(STRING_SERDE, EMPLOYEE_JSON_SERDE));

        KStream<String, EmployeeHistory> employeeHistoryKStream = streamsBuilder
                .stream(EMPLOYMENT_HISTORY_TOPIC, Consumed.with(STRING_SERDE, EMPLOYEE_HISTORY_JSON_SERDE));


        ValueJoiner<Employee, Department, Employee> employeeValueJoiner = (e1, d1) -> {
            e1.setDepartment(d1);
            return e1;
        };

        employeeHistoryKStream
                .peek((key, value) -> log.info(value.toString()))
                .map((key, value) -> new KeyValue<>(value.getEmployeeId(), getEmpHistEmp(value)))
                .to(EMPLOYMENT_MERGE_TOPIC, Produced.with(STRING_SERDE, EMPLOYEE_JSON_SERDE));

        KStream<String, Employee> employeeMergeKStream = streamsBuilder.stream(EMPLOYMENT_MERGE_TOPIC, Consumed.with(STRING_SERDE, EMPLOYEE_JSON_SERDE));

        employeeMergeKStream
                .groupByKey(Grouped.with(STRING_SERDE, EMPLOYEE_JSON_SERDE))
                .aggregate(Employee::new, (key, employeeHistory, employee1) -> {
                    ArrayList<EmployeeHistory> employeeHistories = new ArrayList<>();
                    if(employee1.getEmployeeHistories() != null) {
                        employeeHistories = employee1.getEmployeeHistories();
                    }
//                    employeeHistories.remove(employeeHistory.getEmployeeHistories().get(0));
                    employeeHistories.add(employeeHistory.getEmployeeHistories().get(0));
                    employee1.setEmployeeHistories(employeeHistories);
                    return employee1;
                }, Materialized.with(STRING_SERDE, EMPLOYEE_JSON_SERDE)).toStream()
                .to("employment.merge", Produced.with(STRING_SERDE, EMPLOYEE_JSON_SERDE));

    }

    private static List<KeyValue<String, EmployeeHistory>> getKeyValues(int randomNum , String empHistId, Employee employee) {
        List<KeyValue<String, EmployeeHistory>> result = new LinkedList<>();
        for(int i=0; i<randomNum; i++ ) {
            EmployeeHistory employeeHistory = new EmployeeHistory();
            employeeHistory.setId(empHistId + i);
            employeeHistory.setEmployerName(employee.getName());
            employeeHistory.setEmployeeId(employee.getId());
            result.add(KeyValue.pair(employeeHistory.getId(), employeeHistory));
        }
        return result;
    }

    private Employee getEmpHistEmp(EmployeeHistory employeeHistory) {
        Employee employee = new Employee();
        employee.setEmployeeHistories(new ArrayList<>(Collections.singletonList(employeeHistory)));
        return employee;
    }

}

