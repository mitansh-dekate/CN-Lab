
#include <iostream>
#include <cstring>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace std;

const int PORT = 8080;
const char* SERVER_IP = "127.0.0.1";

int main() {
    int clientSocket;
    sockaddr_in serverAddress{};
    char buffer[1024];

    // Create TCP socket
    clientSocket = socket(AF_INET, SOCK_STREAM, 0);

    if (clientSocket == -1) {
        perror("Socket creation failed");
        return 1;
    }

    // Configure server address
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(PORT);
    inet_pton(AF_INET, SERVER_IP, &serverAddress.sin_addr);

    // Connect to server
    if (connect(clientSocket, (sockaddr*)&serverAddress,
                sizeof(serverAddress)) == -1) {
        perror("Connection failed");
        close(clientSocket);
        return 1;
    }

    cout << "Connected to chat server!\n";
    cout << "Type 'exit' to end the chat.\n";

    while (true) {
        string message;

        // Input and send client message
        cout << "\nYou (Client): ";
        getline(cin, message);

        if (send(clientSocket, message.c_str(),
                 message.size(), 0) == -1) {
            perror("Send failed");
            break;
        }

        // Receive server reply
        memset(buffer, 0, sizeof(buffer));
        ssize_t bytes = recv(clientSocket, buffer,
                             sizeof(buffer) - 1, 0);

        if (bytes <= 0) {
            cout << "Server disconnected.\n";
            break;
        }

        buffer[bytes] = '\0';
        cout << "Server: " << buffer << '\n';

        if (message == "exit")
            break;

        if (string(buffer) == "exit")
            break;
    }

    // Close client socket
    close(clientSocket);

    cout << "Client closed.\n";
    return 0;
}
