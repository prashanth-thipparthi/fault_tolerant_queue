1. We can compile the project using maven.

mvn clean install.

2. We have to run the servers first with 5 different port numbers as arguments.

3. Next we have to tun the client and select the server to connect to.

4. Perform the operations on the queue.

5. Verify the queue by connecting to a different server apart from the first server.

What works:

Distributed fault tolerant feature of the queue should work.

Issues are with concurrency of the code.
