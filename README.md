# Fault-tolerant Distributed Queue with Sequencer based Protocol

Implemenation of faul tolerant and distributed queue with using sequencer based protocol.

Detailed write up [link](https://github.com/prashanth-thipparthi/fault_tolerant_queue/blob/master/ProgrammingAssignment2.pdf "Title").

# Compiling code
mvn clean install.

# Running Program.
## Run the servers using the below command by changing port numers
mvn exec:java@follower -Dexec.args="<port_number>" &
## Run the client using the following command.
mvn exec:java@client

## How it works.
1. We have to run the servers first with 5 different port numbers as arguments.

2. Next we have to run the client and select the server to connect to.

3. Perform the operations on the queue.

4. Verify the queue by connecting to a different server apart from the first server.
