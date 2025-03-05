## Candidate: Dursun Satiroglu

### Description

A small application that consumes and returns messages from Kafka given parameters configured by a REST endpoint.

### Usage
#### Build and testing:

    ./gradlew clean build

#### Running the app:
    
    ./gradlew clean build bootRun

#### Interacting with the app:

Call the endpoint /topic/<topic-name> or /topic/<topic-name>/<offset>. A count paramater is optional, as is the offset param.

Example:
    
    curl -XGET "http://localhost:8080/topic/people-topic/15?count=10"
    # or
    curl -XGET "http://localhost:8080/topic/people-topic
