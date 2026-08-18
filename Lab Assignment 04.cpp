#include <iostream>
using namespace std;

#define INF 999

void displayTables(int dist[][10], int n) {
    cout << "\nFinal Routing Tables:\n";

    for (int i = 0; i < n; i++) {
        cout << "\nRouter " << i + 1 << ":\n";
        cout << "Destination\tCost\n";

        for (int j = 0; j < n; j++) {
            cout << j + 1 << "\t\t";

            if (dist[i][j] == INF)
                cout << "INF";
            else
                cout << dist[i][j];

            cout << endl;
        }
    }
}

void distanceVector(int dist[][10], int n) {
    bool updated;

    do {
        updated = false;

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                for (int k = 0; k < n; k++) {
                    if (dist[i][k] != INF && dist[k][j] != INF) {
                        if (dist[i][j] > dist[i][k] + dist[k][j]) {
                            dist[i][j] = dist[i][k] + dist[k][j];
                            updated = true;
                        }
                    }
                }
            }
        }
    } while (updated);
}

void inputNetwork(int dist[][10], int n) {
    cout << "Enter the cost matrix:\n";
    cout << "(Enter 999 if there is no direct connection)\n\n";

    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            cin >> dist[i][j];
        }
    }
}

int main() {
    int n;
    int dist[10][10];

    cout << "Distance Vector Routing Algorithm\n";
    cout << "Enter number of routers: ";
    cin >> n;

    inputNetwork(dist, n);
    distanceVector(dist, n);
    displayTables(dist, n);

    return 0;
}
