#include <iostream>
using namespace std;

#define INF 999

void displayTables(int dist[][10], int next[][10], int n) {
    cout << "\nFinal Routing Tables:\n";

    for (int i = 0; i < n; i++) {
        cout << "\nRouter " << i + 1 << ":\n";
        cout << "Destination\t\tCost\tNext Hop\n";

        for (int j = 0; j < n; j++) {
            cout << "R" << j + 1 << "\t\t";

            if (dist[i][j] == INF) {
                cout << "INF\t-\n";
            } else {
                cout <<"\t\t"<< dist[i][j] << "\t\tR";

                if (i == j)
                    cout << i + 1;
                else
                    cout << next[i][j] + 1;

                cout << endl;
            }
        }
    }
}

void distanceVector(int dist[][10], int next[][10], int n) {
    bool updated;

    do {
        updated = false;

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                for (int k = 0; k < n; k++) {
                    if (dist[i][k] != INF && dist[k][j] != INF) {
                        if (dist[i][j] > dist[i][k] + dist[k][j]) {
                            dist[i][j] = dist[i][k] + dist[k][j];
                            next[i][j] = next[i][k];
                            updated = true;
                        }
                    }
                }
            }
        }
    } while (updated);
}

void inputNetwork(int dist[][10], int next[][10], int n) {
    cout << "Enter the cost matrix:\n";
    cout << "(Enter 999 if there is no direct connection)\n\n";

    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            cin >> dist[i][j];

            if (i == j)
                next[i][j] = i;
            else if (dist[i][j] != INF)
                next[i][j] = j;
            else
                next[i][j] = -1;
        }
    }
}

void printShortestPath(int source, int destination, int dist[][10],
                       int next[][10]) {

    if (dist[source][destination] == INF) {
        cout << "\nNo path exists between the selected routers.\n";
        return;
    }

    cout << "\nShortest Path: R" << source + 1;

    int current = source;

    while (current != destination) {
        current = next[current][destination];
        cout << " -> R" << current + 1;
    }

    cout << "\nShortest Distance: " << dist[source][destination] << endl;
}

int main() {
    int n;
    int dist[10][10];
    int next[10][10];
    int source, destination;

    cout << "Distance Vector Routing Algorithm\n";
    cout << "Enter number of routers: ";
    cin >> n;

    inputNetwork(dist, next, n);
    distanceVector(dist, next, n);
    displayTables(dist, next, n);

    cout << "\nEnter source router (1-" << n << "): ";
    cin >> source;

    cout << "Enter destination router (1-" << n << "): ";
    cin >> destination;

    source--;
    destination--;

    if (source < 0 || source >= n || destination < 0 || destination >= n) {
        cout << "\nInvalid router number!\n";
        return 0;
    }

    printShortestPath(source, destination, dist, next);

    return 0;
}
