#include <iostream>

#include "fs_helper.h"

int main(int argc, char **argv) {
  std::string filename = "test.txt";
  ExclusiveInputFileStream file2(filename);
  std::string line;
  while (std::getline(file2, line)) {
    std::cout << line << std::endl;
  }

  ExclusiveOutputFileStream file(filename);

  file << "Hello, World!" << std::endl;
  file << "This is a test file." << std::endl;
  file << "Goodbye, World!" << std::endl;
  file << "This should not be visible." << std::endl;
  file << "The file should not be overwritten." << std::endl;
};