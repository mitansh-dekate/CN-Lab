
#include <iostream>
#include <string>
#include <cstring>
#include <cstdio>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

using namespace std;

const int PORT = 8080;

// Function to create and configure the server socket
int createServerSocket() {
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);

    if (serverSocket == -1) {
        perror("Socket creation failed");
        return -1;
    }

    int option = 1;
    setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR,
               &option, sizeof(option));

    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(PORT);

    // Bind socket to IP address and port
    if (::bind(serverSocket,
               reinterpret_cast<sockaddr*>(&serverAddress),
               sizeof(serverAddress)) == -1) {
        perror("Bind failed");
        close(serverSocket);
        return -1;
    }

    // Listen for incoming connections
    if (listen(serverSocket, 5) == -1) {
        perror("Listen failed");
        close(serverSocket);
        return -1;
    }

    cout << "Server listening on port " << PORT << "...\n";
    return serverSocket;
}

// Function to communicate with the client
void chatWithClient(int clientSocket) {
    char buffer[1024];

    cout << "Client connected!\n";
    cout << "Type 'exit' to end the chat.\n";

    while (true) {
        memset(buffer, 0, sizeof(buffer));

        // Receive message from client
        ssize_t bytesReceived = recv(
            clientSocket, buffer, sizeof(buffer) - 1, 0
        );

        if (bytesReceived <= 0) {
            cout << "Client disconnected.\n";
            break;
        }

        buffer[bytesReceived] = '\0';
        string message(buffer);

        cout << "\nClient: " << message << '\n';

        if (message == "exit") {
            string reply = "Goodbye!";
            send(clientSocket, reply.c_str(), reply.size(), 0);
            break;
        }

        // Send reply to client
        string reply;
        cout << "You (Server): ";
        getline(cin, reply);

        if (send(clientSocket, reply.c_str(),
                 reply.size(), 0) == -1) {
            perror("Send failed");
            break;
        }

        if (reply == "exit")
            break;
    }
}

// Main function
int main() {
    int serverSocket = createServerSocket();

    if (serverSocket == -1)
        return 1;

    sockaddr_in clientAddress{};
    socklen_t clientLength = sizeof(clientAddress);

    // Accept client connection
    int clientSocket = accept(
        serverSocket,
        reinterpret_cast<sockaddr*>(&clientAddress),
        &clientLength
    );

    if (clientSocket == -1) {
        perror("Accept failed");
        close(serverSocket);
        return 1;
    }

    chatWithClient(clientSocket);

    // Close sockets
    close(clientSocket);
    close(serverSocket);

    cout << "Server closed.\n";
    return 0;
}
