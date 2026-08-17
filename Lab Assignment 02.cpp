#include <iostream>
#include <cmath>
using namespace std;

#define MAX 100

int calculateParityBits(int n) {
    int r = 0;

    while (pow(2, r) < (n + r + 1))
        r++;

    return r;
}

void inputData(int data[], int n) {
    cout << "Enter " << n << "-bit binary message:\n";

    for (int i = n; i >= 1; i--)
        cin >> data[i];
}

void generateHamming(int data[], int hamming[], int n, int r) {
    int j = 1, k = 1;
    int total = n + r;

    for (int i = 1; i <= total; i++) {
        if (i == pow(2, k - 1)) {
            hamming[i] = 0;
            k++;
        }
        else {
            hamming[i] = data[j];
            j++;
        }
    }
}

void calculateParity(int hamming[], int total, int r) {
    for (int i = 0; i < r; i++) {
        int parity = 0;
        int position = pow(2, i);

        for (int j = 1; j <= total; j++) {
            if (j & position)
                parity ^= hamming[j];
        }

        hamming[position] = parity;
    }
}

void displayCode(int code[], int total) {
    for (int i = total; i >= 1; i--)
        cout << code[i];

    cout << endl;
}

void inputReceived(int received[], int total) {
    cout << "\nEnter Received Code (" << total << " bits): ";

    for (int i = total; i >= 1; i--)
        cin >> received[i];

    cout << "\nReceived Code : ";
    displayCode(received, total);
}

int countChanges(int original[], int received[], int total) {
    int count = 0;

    for (int i = 1; i <= total; i++) {
        if (original[i] != received[i])
            count++;
    }

    return count;
}

int calculateSyndrome(int received[], int total, int r) {
    int syndrome = 0;

    cout << "\nSyndrome Bits:\n";

    for (int i = 0; i < r; i++) {
        int parity = 0;
        int position = pow(2, i);

        for (int j = 1; j <= total; j++) {
            if (j & position)
                parity ^= received[j];
        }

        cout << "S" << position << " = " << parity << endl;

        if (parity)
            syndrome += position;
    }

    return syndrome;
}

void correctError(int received[], int total, int position) {
    received[position] ^= 1;
}

int main() {
    int n, r, total;
    int data[MAX], hamming[MAX], received[MAX];
    int syndrome;

    cout << "Enter number of data bits: ";
    cin >> n;

    inputData(data, n);

    r = calculateParityBits(n);
    total = n + r;

    cout << "\nNumber of parity bits = " << r << endl;

    generateHamming(data, hamming, n, r);
    calculateParity(hamming, total, r);

    cout << "\nEncoded Message : ";
    displayCode(hamming, total);

    inputReceived(received, total);

    if (countChanges(hamming, received, total) > 1)
        cout << "\nWarning: More than one bit has changed.\n";

    syndrome = calculateSyndrome(received, total, r);

    if (syndrome == 0) {
        cout << "\nNo Error Detected.\n";
    }
    else {
        cout << "\nError at Position = " << syndrome << endl;

        correctError(received, total, syndrome);

        cout << "\nCorrected Code : ";
        displayCode(received, total);
    }

    return 0;
}