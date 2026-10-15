
#include <iostream>
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
    string message;
    char buffer[1024];

    sockaddr_in serverAddress{};

    // 1. Create UDP socket
    clientSocket = socket(AF_INET, SOCK_DGRAM, 0);

    if (clientSocket == -1) {
        perror("Socket creation failed");
        return 1;
    }

    // 2. Configure server address
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(PORT);
    inet_pton(AF_INET, SERVER_IP, &serverAddress.sin_addr);

    // 3. Input message
    cout << "Enter a string in uppercase: ";
    getline(cin, message);

    // 4. Send message to server
    if (sendto(clientSocket, message.c_str(), message.size(), 0,
               (sockaddr*)&serverAddress,
               sizeof(serverAddress)) == -1) {
        perror("Send failed");
        close(clientSocket);
        return 1;
    }

    // 5. Receive reply
    sockaddr_in replyAddress{};
    socklen_t replyLength = sizeof(replyAddress);

    ssize_t bytes = recvfrom(
        clientSocket, buffer, sizeof(buffer) - 1, 0,
        (sockaddr*)&replyAddress, &replyLength
    );

    if (bytes == -1) {
        perror("Receive failed");
        close(clientSocket);
        return 1;
    }

    buffer[bytes] = '\0';

    // 6. Display result
    cout << "Lowercase string: " << buffer << '\n';

    // 7. Close socket
    close(clientSocket);

    return 0;
}
