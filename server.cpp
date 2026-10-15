
#include <iostream>
#include <cstring>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

using namespace std;

const int PORT = 8080;

int main() {
    int serverSocket, clientSocket;
    sockaddr_in serverAddress{}, clientAddress{};
    socklen_t clientLength = sizeof(clientAddress);
    char buffer[1024];

    // Create TCP socket
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);

    if (serverSocket == -1) {
        perror("Socket creation failed");
        return 1;
    }

    // Configure server address
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(PORT);

    // Bind socket
    if (bind(serverSocket, (sockaddr*)&serverAddress,
             sizeof(serverAddress)) == -1) {
        perror("Bind failed");
        close(serverSocket);
        return 1;
    }

    // Listen for client
    if (listen(serverSocket, 5) == -1) {
        perror("Listen failed");
        close(serverSocket);
        return 1;
    }

    cout << "Server listening on port " << PORT << "...\n";

    // Accept client connection
    clientSocket = accept(serverSocket,
                          (sockaddr*)&clientAddress,
                          &clientLength);

    if (clientSocket == -1) {
        perror("Accept failed");
        close(serverSocket);
        return 1;
    }

    cout << "Client connected!\n";
    cout << "Type 'exit' to end the chat.\n";

    while (true) {
        // Receive client message
        memset(buffer, 0, sizeof(buffer));
        ssize_t bytes = recv(clientSocket, buffer,
                             sizeof(buffer) - 1, 0);

        if (bytes <= 0) {
            cout << "Client disconnected.\n";
            break;
        }

        buffer[bytes] = '\0';
        cout << "\nClient: " << buffer << '\n';

        if (string(buffer) == "exit") {
            string reply = "Goodbye!";
            send(clientSocket, reply.c_str(), reply.size(), 0);
            break;
        }

        // Send server reply
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

    // Close sockets
    close(clientSocket);
    close(serverSocket);

    cout << "Server closed.\n";
    return 0;
}
