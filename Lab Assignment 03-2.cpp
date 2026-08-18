#include <iostream>
#include <sstream>
#include <cmath>
#include <vector>
#include <iomanip>
using namespace std;

// Convert IP address string to 32-bit integer
unsigned int ipToInt(string ip) {
    stringstream ss(ip);
    string part;
    unsigned int result = 0;

    for (int i = 0; i < 4; i++) {
        getline(ss, part, '.');
        result = (result << 8) | stoi(part);
    }

    return result;
}

// Convert 32-bit integer to IP address
string intToIP(unsigned int ip) {
    return to_string((ip >> 24) & 255) + "." +
           to_string((ip >> 16) & 255) + "." +
           to_string((ip >> 8) & 255) + "." +
           to_string(ip & 255);
}

// Determine IP class
char getClass(int firstOctet) {
    if (firstOctet >= 1 && firstOctet <= 126)
        return 'A';
    else if (firstOctet >= 128 && firstOctet <= 191)
        return 'B';
    else if (firstOctet >= 192 && firstOctet <= 223)
        return 'C';
    else if (firstOctet >= 224 && firstOctet <= 239)
        return 'D';
    else
        return 'E';
}

// Get default prefix based on class
int getDefaultPrefix(char ipClass) {
    switch (ipClass) {
        case 'A': return 8;
        case 'B': return 16;
        case 'C': return 24;
        default: return 0;
    }
}

// Convert prefix length to subnet mask
unsigned int prefixToMask(int prefix) {
    if (prefix == 0)
        return 0;

    return 0xFFFFFFFF << (32 - prefix);
}

int main() {
    string ip;
    int requiredSubnets;

    cout << "Enter IP Address: ";
    cin >> ip;

    cout << "Enter required number of subnets: ";
    cin >> requiredSubnets;

    // Convert IP to integer
    unsigned int ipInt = ipToInt(ip);

    // Find first octet
    int firstOctet = (ipInt >> 24) & 255;

    // Find class
    char ipClass = getClass(firstOctet);

    if (ipClass == 'D' || ipClass == 'E') {
        cout << "\nClass " << ipClass
             << " does not support normal subnetting.\n";
        return 0;
    }

    // Default prefix
    int defaultPrefix = getDefaultPrefix(ipClass);

    // Find number of bits to borrow
    int borrowedBits = 0;

    while ((1 << borrowedBits) < requiredSubnets)
        borrowedBits++;

    // New prefix
    int newPrefix = defaultPrefix + borrowedBits;

    if (newPrefix > 30) {
        cout << "\nToo many subnets requested for this IP class.\n";
        return 0;
    }

    // Calculate masks
    unsigned int defaultMask = prefixToMask(defaultPrefix);
    unsigned int newMask = prefixToMask(newPrefix);

    // Number of actual subnets
    int totalSubnets = pow(2, borrowedBits);

    // Hosts per subnet
    int hostBits = 32 - newPrefix;
    unsigned int hostsPerSubnet = pow(2, hostBits) - 2;

    // Total usable hosts
    unsigned long long totalHosts =
        (unsigned long long)totalSubnets * hostsPerSubnet;

    // Network address
    unsigned int networkAddress = ipInt & defaultMask;

    // IP range of the entire network
    unsigned int firstIP = networkAddress;
    unsigned int lastIP = networkAddress + pow(2, 32 - defaultPrefix) - 1;

    // ---------------- OUTPUT ----------------

    cout << "\n============================================\n";
    cout << "             SUBNETTING DETAILS\n";
    cout << "============================================\n";

    // 1. IP Address
    cout << "\nIP Address        : " << ip << endl;

    // 2. Class
    cout << "IP Class          : Class " << ipClass << endl;

    // 3. Default Mask
    cout << "Default Subnet    : "
         << intToIP(defaultMask)
         << " /" << defaultPrefix << endl;

    // 4. New Mask
    cout << "New Subnet Mask   : "
         << intToIP(newMask)
         << " /" << newPrefix << endl;

    cout << "Bits Borrowed     : " << borrowedBits << endl;

    // 5. IP Address Range
    cout << "\n--------------------------------------------\n";
    cout << "IP ADDRESS RANGE\n";
    cout << "--------------------------------------------\n";

    cout << "Network Address   : " << intToIP(firstIP) << endl;
    cout << "First IP          : " << intToIP(firstIP + 1) << endl;
    cout << "Last IP           : " << intToIP(lastIP - 1) << endl;
    cout << "Broadcast Address : " << intToIP(lastIP) << endl;

    // 6. Subnet information
    cout << "\n--------------------------------------------\n";
    cout << "SUBNET INFORMATION\n";
    cout << "--------------------------------------------\n";

    cout << "Required Subnets  : " << requiredSubnets << endl;
    cout << "Actual Subnets    : " << totalSubnets << endl;
    cout << "Hosts per Subnet  : " << hostsPerSubnet << endl;
    cout << "Total Usable Hosts: " << totalHosts << endl;

    // 7. List all subnets
    cout << "\n--------------------------------------------\n";
    cout << "LIST OF ALL SUBNETS\n";
    cout << "--------------------------------------------\n";

    unsigned int subnetSize = pow(2, hostBits);

    for (int i = 0; i < totalSubnets; i++) {

        unsigned int subnetNetwork =
            networkAddress + (i * subnetSize);

        unsigned int subnetBroadcast =
            subnetNetwork + subnetSize - 1;

        unsigned int firstHost = subnetNetwork + 1;
        unsigned int lastHost = subnetBroadcast - 1;

        cout << "\nSubnet " << i + 1 << ":\n";
        cout << "  Network   : " << intToIP(subnetNetwork) << endl;
        cout << "  First Host: " << intToIP(firstHost) << endl;
        cout << "  Last Host : " << intToIP(lastHost) << endl;
        cout << "  Broadcast : " << intToIP(subnetBroadcast) << endl;
    }

    cout << "\n============================================\n";

    return 0;
}
