#include <iostream>
#include <fstream>
#include <random>
#include <string>

void split_file(const std::string& input_file, const std::string& out_10, 
                const std::string& out_50, const std::string& out_100) {
    std::ifstream in(input_file);
    std::ofstream f10(out_10), f50(out_50), f100(out_100);
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);

    std::string line;
    long long count_10 = 0, count_50 = 0, count_100 = 0;

    while (std::getline(in, line)) {
        f100 << line << '\n';  // 100%
        count_100++;

        if (dis(gen) < 0.1) {  // 10%
            f10 << line << '\n';
            count_10++;
        }
        if (dis(gen) < 0.5) {  // 50%
            f50 << line << '\n';
            count_50++;
        }
    }

    std::cout << "10%: " << count_10 << " lignes\n";
    std::cout << "50%: " << count_50 << " lignes\n";
    std::cout << "100%: " << count_100 << " lignes\n";
}

int main() {
    split_file("/mnt/c/Users/Youcode/Pictures/benchmark/data/measurements.txt", 
               "/mnt/c/Users/Youcode/Pictures/benchmark/data/measurement_10_percent.txt",
               "/mnt/c/Users/Youcode/Pictures/benchmark/data/measurement_50_percent.txt",
               "/mnt/c/Users/Youcode/Pictures/benchmark/data/measurement_100_percent.txt");
    return 0;
}
