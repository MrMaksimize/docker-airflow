import struct
import socket
import sys

if len(sys.argv) != 4:
   print "Usage: " + sys.argv[0] + " <host> <port> <request>"
   exit(1)

TCP_IP = sys.argv[1] # The host name to send the request to
TCP_PORT = sys.argv[2] # The port to send the request to

keyword_str = 'JOB_SUBMITTER_SERVICE'
request_str = sys.argv[3] # the parameters to pass to FME on the command line

# pack the messages we are going to send
keyword = struct.pack('!i' + str(len(keyword_str)) + 's', len(keyword_str), keyword_str)
request = struct.pack('!i' + str(len(request_str)) + 's', len(request_str), request_str)

# open the socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((TCP_IP, int(TCP_PORT)))

# send the request
s.send(keyword)
s.send(request)

# receieve the size of the message which is the first 4 bytes
data = s.recv(4)
recv_size = struct.unpack('!i', data)[0]

# recieve the message
data = s.recv(recv_size)
result_message = struct.unpack('!' + str(recv_size) + 's', data)[0]

# print what we got back from the engine
print result_message

# close the socket
s.close()
