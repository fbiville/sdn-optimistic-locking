# Optimistic locking with SDN

The project is made of two modules:

 - `sdn-6` for the latest Spring Data Neo4j (SDN)
 - `sdn-ogm` for SDN-OGM
 
They are both built upon the `movies` dataset and define the same integration test around a specific movie deletion.
One the tests fails, as you can see by running `mvn test`.
