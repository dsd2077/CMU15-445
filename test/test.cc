#include <iostream>
#include <bitset>

using namespace std;

int main() {
  constexpr int num_words = 10;
  constexpr int num_bits = 4;
  for (int i = 0; i < num_words; i++) {
    std::string key = std::bitset<num_bits>(i).to_string();
    cout << key << endl;
  }
}