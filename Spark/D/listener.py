import socket
import time

# First we set up our socket object
s = socket.socket()

# Tell the socket what your host is (it's set to local right now)
host = "127.0.0.1" # socket.gethostname() 

# Typically ports 0-1023 are reserved for the operating system, so stay above that
port = 1234 

# Bind the host and port 
s.bind((host, port))

# Send a message so we know this actually happened
print("Listening on port: %s" % str(port))

s.listen(5)

clientsocket, address = s.accept() # address is ip address

# Let's print it so we can confirm that when we are at the command line

print("Received request from: " + str(address)," connection created.")

data_stream = ["test1, ","test2, ","test3, ","test4, ","test5, "]

for data in data_stream:
    
    # convert to bytes
    print("Sending:", data)
    bytes_data = bytes(data, 'utf-8')
    clientsocket.send(bytes_data) # send to the client socket
    # We will sleep for 2 second here to demostrate how data can come faster than we are collecting it
    time.sleep(2)

clientsocket.close()