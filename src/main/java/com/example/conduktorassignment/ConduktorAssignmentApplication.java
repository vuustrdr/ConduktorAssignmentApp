package com.example.conduktorassignment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ConduktorAssignmentApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConduktorAssignmentApplication.class, args);
    }

}

//// Notes
// didnt realize the count separation to change for per partition
// fetching all and iterating over was initial idea
// seemed really inefficient so looked into more consumer properties, decided to limit based on that
// saw that it wasnt split well over partitions on smaller values so wanted to try and dynamically change max poll records on an even split
// but thtas not possible without creating and recreating ocnsumers which is inefficient in itself.
// thinking around app
// adaption of more production thinking, shhould continusioly poll not just once, therefore cant return stuff
// need something like a queue to store messages and retrieve synchronously


//// Possible extensions
// add a new partition
// adding auth
// ways of filtering through
// upload something new
// new rest endpoint (ask for specifics)
// design extensions (ask for specifics)
// testing edge cases
