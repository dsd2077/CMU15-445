#include <iostream>
#include <vector>
#include <list>
#include <memory>
#include <map>

using namespace std;

class Bucket {
   public:
    explicit Bucket(size_t size, int depth = 0): size_(size), depth_(depth){}
    void Print() { cout << size_ << " " << depth_ << endl; }

   private:
    // TODO(student): You may add additional private members and helper functions
    size_t size_;
    int depth_;
  };

std::vector<std::shared_ptr<Bucket>> dir;
std::map<std::shared_ptr<Bucket>, int> pos;

void add_bucket() {
  auto new_bucket1 = std::make_shared<Bucket>(3, 3);
  auto new_bucket2 = std::make_shared<Bucket>(4, 4);
  cout << new_bucket1 << endl;
  cout << new_bucket2 << endl;

  cout << new_bucket1.use_count() << endl;
  cout << new_bucket2.use_count() << endl;

  std::vector<std::shared_ptr<Bucket>> new_dir(dir.size()*2);
  new_dir[0] = dir[0];
  new_dir[1] = dir[1];
  new_dir[2] = new_bucket1;
  new_dir[3] = new_bucket2;
  pos[new_bucket1] = 2;
  pos[new_bucket2] = 3;
  dir = new_dir;
  cout << new_bucket1.use_count() << endl;
  cout << new_bucket2.use_count() << endl;
}

int main() {

  if (dir.empty()) {
    dir.emplace_back(std::make_shared<Bucket>(1, 1));
    dir.emplace_back(std::make_shared<Bucket>(2, 2));
  }

  for (auto & ptr : dir) {
    ptr->Print();
  }

  cout << "-------------" << endl;
  add_bucket();

  for (auto & ptr : dir) {
    ptr->Print();
  }

  cout << dir[2] << endl;
  cout << dir[3] << endl;

  for (const auto&[ptr, p] : pos) {
    cout << ptr << "   "  << ptr.use_count() << "   " << p << endl;
  }
  

  return 0;
}
