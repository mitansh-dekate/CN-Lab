
#include <iostream>
#include <string>
#include <cctype>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

using namespace std;

const int PORT = 8080;

// Convert uppercase letters to lowercase
string convertToLower(string message) {
    for (char &ch : message) {
        ch = tolower(static_cast<unsigned char>(ch));
    }
    return message;
}

int main() {
    int serverSocket;
    char buffer[1024];

    sockaddr_in serverAddress{}, clientAddress{};
    socklen_t clientLength = sizeof(clientAddress);

    // 1. Create UDP socket
    serverSocket = socket(AF_INET, SOCK_DGRAM, 0);

    if (serverSocket == -1) {
        perror("Socket creation failed");
        return 1;
    }

    // 2. Configure server address
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(PORT);

    // 3. Bind socket
    if (bind(serverSocket, (sockaddr*)&serverAddress,
             sizeof(serverAddress)) == -1) {
        perror("Bind failed");
        close(serverSocket);
        return 1;
    }

    cout << "UDP Server listening on port "
         << PORT << "...\n";

    // 4. Receive message from client
    ssize_t bytes = recvfrom(
        serverSocket, buffer, sizeof(buffer) - 1, 0,
        (sockaddr*)&clientAddress, &clientLength
    );

    if (bytes == -1) {
        perror("Receive failed");
        close(serverSocket);
        return 1;
    }

    buffer[bytes] = '\0';
    string message(buffer);

    cout << "Received from client: " << message << '\n';

    // 5. Process message
    string result = convertToLower(message);

    // 6. Send reply to client
    if (sendto(serverSocket, result.c_str(), result.size(), 0,
               (sockaddr*)&clientAddress,
               clientLength) == -1) {
        perror("Send failed");
        close(serverSocket);
        return 1;
    }

    cout << "Converted string: " << result << '\n';

    // 7. Close socket
    close(serverSocket);

    return 0;
}
