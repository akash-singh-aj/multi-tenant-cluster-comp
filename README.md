Q1. What is the rationale for the technologies you have decided to use?
Akka actors were chosen for their actor model, which handles concurrency through lightweight, isolated units communicating via asynchronous messages, avoiding locks, deadlocks, and shared mutable state issues common in traditional threading. 
This fits distributed backend systems like apache spark, or any other cluster computing platform enabling high throughput and fault tolerance via supervision hierarchies.

Q2. What would you have done differently if you had more time?
With additional time, the project would migrate to Go with goroutines for simpler operations, lighter memory use, and faster I/O-bound throughput,
Apache Pekko could replace Akka for open-source stability, or lighter options like Vert.x for non-clustered concurrency if distribution proves overkill. 
More focus would go into comprehensive testing of actor interactions and production metrics integration with Prometheus, matching your monitoring background.

Q3. What is the rationale for the design choices that you have made?
The design focused on strong actor encapsulation to keep state isolated, with message-driven communication to stay location-transparent—so the system could scale from local to distributed without major changes. 
Hierarchical supervision was used to improve resilience, aligning with reactive design principles and Domain-Driven Design.