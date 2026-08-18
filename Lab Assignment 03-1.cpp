#include <iostream>
#include <string>
#include <cmath>

using namespace std;

// Function to extract the first octet from the IP address
int getFirstOctet(string ipAddress){
    int firstOctet = 0;
    for (int i = 0; i < ipAddress.length(); i++){
        if (ipAddress[i] == '.'){
            break;
        }
        firstOctet = firstOctet * 10 + (ipAddress[i] - '0');
    }
    return firstOctet;
}

// Function to determine the class of the IP address
char findClass(int firstOctet){
    if (firstOctet >= 1 && firstOctet <= 126){
        return 'A';
    }
    else if (firstOctet >= 128 && firstOctet <= 191){
        return 'B';
    }
    else if (firstOctet >= 192 && firstOctet <= 223){
        return 'C';
    }
    else if (firstOctet >= 224 && firstOctet <= 239){
        return 'D';
    }
    else if (firstOctet >= 240 && firstOctet <= 255){
        return 'E';
    }

    return 'X'; // Invalid or special address
}

// Function to display network information
void displayNetworkDetails(char ipClass)
{
    string firstOctetDecimalRange;
    string firstOctetBinaryRange;
    string ipRange;
    string subnetMask;
    long long hostsPerNetwork = 0;
    long long numberOfNetworks = 0;
    

    switch (ipClass){
        case 'A':
            firstOctetDecimalRange = "0 to 127";
            firstOctetBinaryRange = "0XXXXXXX";
            ipRange = "0.0.0.0 to 127.255.255.255";
            subnetMask = "255.0.0.0";
            hostsPerNetwork = pow(2, 24) - 2;
            numberOfNetworks = 126;
            break;

        case 'B':
            firstOctetDecimalRange = "128 to 191";
            firstOctetBinaryRange = "10XXXXXX";
            ipRange = "128.0.0.0 to 191.255.255.255";
            subnetMask = "255.255.0.0";
            hostsPerNetwork = pow(2, 16) - 2;
            numberOfNetworks = pow(2, 14);
            break;

        case 'C':
            firstOctetDecimalRange = "192 to 223";
            firstOctetBinaryRange = "110XXXXX";
            ipRange = "192.0.0.0 to 223.255.255.255";
            subnetMask = "255.255.255.0";
            hostsPerNetwork = pow(2, 8) - 2;
            numberOfNetworks = pow(2, 21);
            break;

        case 'D':
            firstOctetDecimalRange = "224 to 239";
            firstOctetBinaryRange = "1110XXXX";
            ipRange = "224.0.0.0 to 239.255.255.255";
            subnetMask = "N/A";
            hostsPerNetwork = 0;
            numberOfNetworks = 0;
            break;

        case 'E':
            firstOctetDecimalRange = "240 to 255";
            firstOctetBinaryRange = "1111XXXX";
            ipRange = "240.0.0.0 to 255.255.255.255";
            subnetMask = "N/A";
            hostsPerNetwork = 0;
            numberOfNetworks = 0;
            break;

        default:
            cout << "Invalid IP Address!" << endl;
            return;
    }
    cout << "-----------------------------" << endl;
    cout << "IP Class          : Class " << ipClass << endl;
    cout << "Range (Decimal)   : " << firstOctetDecimalRange << endl; 
    cout << "Range (Binary)    : " << firstOctetBinaryRange << endl;
    cout << "IP Range          : " << ipRange << endl;
    cout << "Subnet Mask       : " << subnetMask << endl;

    if (ipClass == 'A' || ipClass == 'B' || ipClass == 'C'){
        
        cout << "Hosts per Network : " << hostsPerNetwork << endl;
        cout << "Number of Networks: " << numberOfNetworks << endl;
    }
    else if (ipClass == 'D'){
        cout << "Purpose           : Multicast" << endl;
    }
    else if (ipClass == 'E'){
        cout << "Purpose           : Experimental/Reserved" << endl;
    }

    cout << "-----------------------------" << endl;
}

void displayDefinition(){
    cout << "Subnetting: Dividing a large network into smaller subnetworks." << endl;

cout << "Subnet Mask: A 32-bit address that identifies the network and host portions of an IP address." << endl;
cout << "Loopback Address: An IP address used to communicate with the local computer itself." << endl;

}
int main()
{
    string ipAddress;

    // Input the complete IP address
    cout << "Enter IPv4 Address: ";
    cin >> ipAddress;

    // Extract the first octet
    int firstOctet = getFirstOctet(ipAddress);

    // Find the IP class
    char ipClass = findClass(firstOctet);

    // Display definition
    displayDefinition();

    // Display all network details
    displayNetworkDetails(ipClass);

    return 0;
}
