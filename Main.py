import socket
import argparse
import sys
import mysql.connector
from mysql.connector import Error


########################################################################
# Server class
########################################################################
class Server:
    HOSTNAME = "0.0.0.0"
    PORT = 4000
    RECV_BUFFER_SIZE = 4096
    MAX_CONNECTION_BACKLOG = 10
    MSG_ENCODING = "utf-8"
    SOCKET_ADDRESS = (HOSTNAME, PORT)
    INPUT_TEXT = ""
    DATABASE_OUTPUT = ""

    def __init__(self):
        self.create_listen_socket()
        self.process_connections_forever()

    def create_listen_socket(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Set socket layer socket options. This allows us to reuse
            # the socket without waiting for any timeouts.
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind(Server.SOCKET_ADDRESS)
            self.socket.listen(Server.MAX_CONNECTION_BACKLOG)
            print("Listening for connections on port {} ...".format(Server.PORT))

        except Exception as msg:
            print(msg)
            sys.exit(1)

    def process_connections_forever(self):
        try:
            while True:
                client = self.socket.accept()
                self.connection_handler(client)
        except Exception as msg:
            print(msg)
        except KeyboardInterrupt:
            print()

    def connection_handler(self, client):
        connection, address_port = client
        print("-" * 72)
        print("Connection received from {} on port {}.".format(address_port[0], address_port[1]))
        while True:
            try:
                # Receive bytes over the TCP connection. This will block
                # until "at least 1 byte or more" is available.
                recv_bytes = connection.recv(Server.RECV_BUFFER_SIZE)

                # If recv returns with zero bytes, the other end of the
                # TCP connection has closed (The other end is probably in
                # FIN WAIT 2 and we are in CLOSE WAIT.). If so, close the
                # server end of the connection and get the next client
                # connection.

                if len(recv_bytes) == 0:
                    print("Closing client connection ... ")
                    connection.close()
                    print("client connection closed")
                    print("Listening for connections on port {} ...".format(Server.PORT))
                    break
                else:
                    self.INPUT_TEXT = recv_bytes.decode(self.MSG_ENCODING)
                    if self.INPUT_TEXT.upper() == "EXIT":
                        print("Closing client connection ... ")
                        connection.close()
                        print("client connection closed")
                        print("Listening for connections on port {} ...".format(Server.PORT))
                        break
                    elif self.INPUT_TEXT[0:8].upper() == "DATABASE":
                        self.connect_to_mysql()
                        connection.sendall(self.DATABASE_OUTPUT.encode(self.MSG_ENCODING))
                    else:
                        self.DATABASE_OUTPUT = "Error: Input command is not supported by Server, " \
                                               "Please check the user manual"
                        connection.sendall(self.DATABASE_OUTPUT.encode(self.MSG_ENCODING))
            except KeyboardInterrupt:
                print()
                print("Closing client connection ... ")
                connection.close()
                break

    def connect_to_mysql(self):
        try:
            connection_sql = mysql.connector.connect(host='localhost', database='lahman2016', user='root', password='4667')
            if connection_sql.is_connected():
                db_Info = connection_sql.get_server_info()
                print("Successfully Connected to MySQL database... MySQL Server version on ", db_Info)

                #Handle command to show all tables
                if self.INPUT_TEXT[9:].upper() == "SHOW TABLES":
                    cursor = connection_sql.cursor(buffered=True)
                    cursor.execute("SHOW TABLES")
                    self.DATABASE_OUTPUT = self.tuple_parser(str(cursor.fetchall()), "), (")

                #Handle command to show attributes
                elif self.INPUT_TEXT[9:25].upper() == "SHOW ATTRIBUTES ":
                    local_query = "DESCRIBE " + self.INPUT_TEXT[16:]
                    cursor = connection_sql.cursor(buffered=True)
                    cursor.execute(local_query)
                    self.DATABASE_OUTPUT = self.tuple_parser(str(cursor.fetchall()), "), (")

                #Handle command to check if tables contain certain attributes from another table
                elif self.INPUT_TEXT[9:17].upper() == "CHECK IF":
                    local_input = self.INPUT_TEXT.split(" ")
                    if local_input[4].upper() == "CONTAINS":
                        local_query = "SELECT {} FROM {} WHERE {} NOT IN (SELECT {} FROM {})".\
                            format(local_input[7], local_input[5], local_input[7], local_input[7], local_input[3])
                        cursor = connection_sql.cursor(buffered=True)
                        cursor.execute(local_query)
                        result = str(cursor.fetchall())
                        if result != "[]":
                            self.DATABASE_OUTPUT = self.tuple_parser(result, "), (")
                        else:
                            self.DATABASE_OUTPUT = "EMPTY"

                #Handle command to add row contains certain attributes value to a table
                elif self.INPUT_TEXT[9:12].upper() == "ADD":
                    local_input = self.INPUT_TEXT.split(" ")
                    local_query = self.query_generator("ADD", local_input)
                    recover_query = self.recovery_query_generator("ADD", local_input)
                    cursor = connection_sql.cursor(buffered=True)
                    local_query = local_query.split(";")
                    for e in local_query:
                        cursor.execute(e)
                        connection_sql.commit()
                    self.f_recovery("write", recover_query)
                    self.DATABASE_OUTPUT = "Database successfully updated"

                #Handle command to delete row contains certain attributes value from a table
                elif self.INPUT_TEXT[9:15].upper() == "DELETE":
                    local_input = self.INPUT_TEXT.split(" ")
                    local_query = self.query_generator("DELETE", local_input)
                    recover_query = self.recovery_query_generator("SELECT", local_input)
                    table_data = ""
                    cursor = connection_sql.cursor(buffered=True)
                    recover_query = recover_query.split(";")
                    #Grab Necessary Table Information into recovery.sql
                    for e in recover_query:
                        cursor.execute(e)
                        table_data = table_data + str(cursor.fetchall())
                    recover_query = self.recovery_query_generator("INSERT", local_input, table_data)
                    local_query = local_query.split(";")
                    ######################################################
                    for e in local_query:
                        cursor.execute(e)
                        connection_sql.commit()
                    self.f_recovery("write", recover_query)
                    self.DATABASE_OUTPUT = "Database successfully updated"

                #Run Database recovery.sql
                elif self.INPUT_TEXT[9:].upper() == "RECOVERY":
                    local_query = open("recovery.sql").read()
                    cursor = connection_sql.cursor(buffered=True)
                    local_query = local_query.split(";")
                    for e in local_query:
                        cursor.execute(e)
                        connection_sql.commit()
                    self.f_recovery("erase")
                    self.DATABASE_OUTPUT = "Database successfully recovered"
                else:
                    self.DATABASE_OUTPUT = "Error: Input command is not supported by Server, " \
                                           "Please check the user manual"

        except Error as e:
            self.DATABASE_OUTPUT = "Error while connecting to MySQL " + str(e)
        #finally:
            #closing database connection.
            #if connection_sql.is_connected():

    def tuple_parser(self, tuple_string: str, separator_1: str, separator_2: str = "'"):
        tuple_list = tuple_string.split(separator_1)
        i = 0
        output_string = ""
        for e in tuple_list:
            e = e.split(separator_2)
            if i == 0:
                output_string = output_string + e[1]
            else:
                output_string = output_string + "," + e[1]
            i = i + 1
        output_string = output_string
        return output_string

    def query_generator(self, mode: str, input: list):
        attributes_list = input[2].split(",")
        query = ""
        if mode == "ADD":
            for e in attributes_list:
                query = query + "INSERT INTO {}({}) VALUES('{}');". \
                        format(input[4], input[6], e)
        elif mode == "DELETE":
            for e in attributes_list:
                query = query + "DELETE FROM {} WHERE {} = '{}';".\
                    format(input[4], input[6], e)
        return query

    def recovery_query_generator(self, mode: str, input: list, table_data=""):
        attributes_list = input[2].split(",")
        query = ""
        if mode == "ADD":
            for e in attributes_list:
                query = query + "DELETE FROM {} WHERE {} = '{}';".\
                    format(input[4], input[6], e)
        elif mode == "SELECT":
            for e in attributes_list:
                query = query + "SELECT * FROM {} WHERE {} = '{}';".\
                    format(input[4], input[6], e)
        elif mode == "INSERT":
            table_data = table_data.split("][")
            table_data[0] = table_data[0][1:]
            table_data[-1] = table_data[-1][:-1]
            for e in table_data:
                if len(e) > 2:
                    query = query + "INSERT INTO {} VALUES {};".\
                    format(input[4], e)
        return query

    def f_recovery(self, mode: str, input=""):
        if mode == "write":
            file = open("recovery.sql", "a+")
            file.write(input)
            file.close()
        elif mode == "erase":
            open("recovery.sql", 'w').close()


########################################################################
# Client class
########################################################################

class Client:
    # Set the server hostname to connect to. If the server and client
    # are running on the same machine, we can use the current
    # hostname.
    SERVER_HOSTNAME = socket.gethostname()
    HOSTNAME = socket.gethostname()
    PORT = 40000
    RECV_BUFFER_SIZE = 4096

    def __init__(self):
        self.send_console_input_forever()

    def get_socket(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((Client.HOSTNAME, Client.PORT))
        except Exception as msg:
            print(msg)
            sys.exit(1)

    def connect_to_server(self):
        try:
            print("-" * 72)
            self.socket.connect((Client.SERVER_HOSTNAME, Server.PORT))
            print("Successfully Connected to the Database Server")
            while True:
                self.get_console_input()
                self.connection_send()
                self.connection_receive()
        except Exception as msg:
            print(msg)
            sys.exit(1)

    def connect_to_server_local(self):
        self.connection_send()
        self.connection_receive()

    def get_console_input(self, optional_input=[]):
        while True:
            if len(optional_input) == 0:
                self.input_text = input("Input: ")
            else:
                user_input = input("Input: ")
                if user_input.upper() == "ADD":
                    self.input_text = "DATABASE {} {} INTO {} ON {}".format(user_input, optional_input[0],
                                                                        optional_input[1], optional_input[3])
                elif user_input.upper() == "DELETE":
                    self.input_text = "DATABASE {} {} FROM {} ON {}".format(user_input, optional_input[0],
                                                                        optional_input[2], optional_input[3])
                elif user_input.upper() == "IGNORE":
                    self.input_text = user_input
                    break
                else:
                    print("The command you typed can not be recognized please re-type")
            if self.input_text != "":
                break

    def send_console_input_forever(self):
        while True:
            try:
                self.get_socket()
                self.connect_to_server()
                print("Closing server connection ... ")
                self.socket.close()

            except (KeyboardInterrupt, EOFError):
                print()
                print("Closing server connection ...")
                self.socket.close()
                sys.exit(1)

    def connection_send(self):
        try:
            self.socket.sendall(self.input_text.encode(Server.MSG_ENCODING))
        except Exception as msg:
            print(msg)
            sys.exit(1)

    def connection_send_byte(self, input_byte):
        try:
            self.socket.sendall(input_byte)
        except Exception as msg:
            print(msg)
            sys.exit(1)

    def connection_receive(self):
        try:
            recvd_bytes = self.socket.recv(Client.RECV_BUFFER_SIZE)
            if len(recvd_bytes) == 0:
                print("Closing server connection ... ")
                self.socket.close()
                sys.exit(1)
            else:
                #No error received:
                if recvd_bytes.decode(Server.MSG_ENCODING)[0:5] != "Error":
                    #Command 'Database Show Tables'
                    if self.input_text.upper() == "DATABASE SHOW TABLES":
                        print("Waiting for Server Response.......\n--------------------------------------------------"
                              "------------------\nThe Database Contains following tables:")
                        print(recvd_bytes.decode(Server.MSG_ENCODING))
                        print("--------------------------------------------------------------------")

                    #Command 'Database Show attributies [Table_Name]
                    elif self.input_text[0:25].upper() == "DATABASE SHOW ATTRIBUTES ":
                        print("Waiting for Server Response.......\n----------------------------------------------------"
                              "----------------\nTable {} contains following attributes".format(self.input_text[25:]))
                        print(recvd_bytes.decode(Server.MSG_ENCODING))
                        print("--------------------------------------------------------------------")

                    #Command 'Database check if [Table_name] contains [Table_name] on Attributes
                    elif self.input_text[0:17].upper() == "DATABASE CHECK IF":
                        print("Waiting for Server Response.......\n"
                              "--------------------------------------------------------------------")
                        local_input = self.input_text.split(" ")

                        #if [table_name] contains all [attribute] from [table_name]
                        if recvd_bytes.decode(Server.MSG_ENCODING) == "EMPTY":
                            print('Table {} contains all attributes {} from Table {}'.
                                  format(local_input[3], local_input[7], local_input[5]))
                            print("--------------------------------------------------------------------")
                        # if [table_name] does not contain all [attribute] from [table_name]
                        else:
                            print('Table {} does not contains following attributes from Table {} on attribute {}'.
                                  format(local_input[3], local_input[5], local_input[7]))
                            print(recvd_bytes.decode(Server.MSG_ENCODING))
                            print("Please suggest what to do: \n"
                                  "1. Input 'Add' to add missing attributes into {}\n"
                                  "2. Input 'Delete' to delete the extra attributes from {}\n"
                                  "3. Input 'Ignore' to ignore the change".
                                  format(local_input[3], local_input[5]))
                            print("--------------------------------------------------------------------")
                            self.get_console_input([recvd_bytes.decode(Server.MSG_ENCODING), local_input[3],
                                                    local_input[5], local_input[7]])
                            if self.input_text.upper() == "IGNORE":
                                print("Ignore the conflict")
                                print("--------------------------------------------------------------------")
                            else:
                                self.connection_send()
                                self.connection_receive()

                    #command "DATABASE ADD [ATTRIBUTE_VALUE] INTO [TABLE NAME] ON [ATTRIBUTE]" and "DATABASE DELETE [ATTRIBUTE_VALUE] FROM [TABLE NAME] ON [ATTRIBUTE]"
                    elif self.input_text[0:12].upper() == "DATABASE ADD" or \
                            self.input_text[0:15].upper() == "DATABASE DELETE":
                        print("Waiting for Server Response.......\n----------------------------------------------------"
                              "----------------")
                        print(recvd_bytes.decode(Server.MSG_ENCODING))
                        print("--------------------------------------------------------------------")

                    #command "Database Recovery"
                    elif self.input_text[0:17].upper() == "DATABASE RECOVERY":
                        print("Waiting for Server Response.......\n----------------------------------------------------"
                              "----------------")
                        print(recvd_bytes.decode(Server.MSG_ENCODING))
                        print("--------------------------------------------------------------------")

                else:
                    print(recvd_bytes.decode(Server.MSG_ENCODING))
        except Exception as msg:
            print(msg)
            sys.exit(1)


########################################################################
# Process command line arguments if this module is run directly.
########################################################################


if __name__ == '__main__':
    roles = {'client': Client, 'server': Server}
    parser = argparse.ArgumentParser()

    parser.add_argument('-r', '--role',
                        choices=roles,
                        help='server or client role',
                        required=True, type=str)

    args = parser.parse_args()
    roles[args.role]()

########################################################################
