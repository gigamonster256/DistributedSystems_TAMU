#pragma once

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

class FileOutputBuffer : public std::streambuf {
 public:
  explicit FileOutputBuffer(FILE* file) : file_(file) {}

 protected:
  int overflow(int ch) override {
    if (ch == EOF) {
      return EOF;
    }
    if (fputc(ch, file_) == EOF) {
      return EOF;
    }
    return ch;
  }

  int sync() override { return fflush(file_) == 0 ? 0 : -1; }

 private:
  FILE* file_;
};

class ExclusiveOutputFileStream : public std::ostream {
 public:
  ExclusiveOutputFileStream(const std::string& filename,
                            std::ios_base::openmode mode = std::ios_base::trunc)
      : std::ostream(nullptr), file_(nullptr), buffer_(nullptr) {
    // Open file in append mode initially until the lock is acquired
    file_ = fopen(filename.c_str(), "a");
    if (file_ == nullptr) {
      throw std::runtime_error("Failed to open file");
    }
    if (flock(fileno(file_), LOCK_EX) == -1) {
      fclose(file_);
      throw std::runtime_error("Failed to lock file");
    }

    // If the file is opened in trunc mode, truncate the file
    if (mode & std::ios_base::trunc) {
      if (freopen(filename.c_str(), "w", file_) == nullptr) {
        fclose(file_);
        throw std::runtime_error("Failed to truncate file");
      }
    }

    // Initialize the custom buffer and set it as the stream buffer
    buffer_ = new FileOutputBuffer(file_);
    this->rdbuf(buffer_);
  }

  ~ExclusiveOutputFileStream() {
    this->flush();  // Flush the stream before releasing the lock
    flock(fileno(file_), LOCK_UN);
    fclose(file_);
    delete buffer_;
  }

 private:
  FILE* file_;
  FileOutputBuffer* buffer_;
};

class FileInputBuffer : public std::streambuf {
 public:
  explicit FileInputBuffer(FILE* file) : file_(file) {}

 protected:
  int underflow() override {
    if (gptr() == egptr()) {  // buffer is empty
      int c = fgetc(file_);
      if (c == EOF) {
        return EOF;
      }
      buffer_ = static_cast<char>(c);
      setg(&buffer_, &buffer_, &buffer_ + 1);  // set buffer pointers
    }
    return traits_type::to_int_type(*gptr());
  }

 private:
  FILE* file_;
  char buffer_;
};

class ExclusiveInputFileStream : public std::istream {
 public:
  ExclusiveInputFileStream(const std::string& filename)
      : std::istream(nullptr), file_(nullptr), buffer_(nullptr) {
    file_ = fopen(filename.c_str(), "r");
    if (file_ == nullptr) {
      throw std::runtime_error("Failed to open file");
    }
    if (flock(fileno(file_), LOCK_EX) == -1) {
      fclose(file_);
      throw std::runtime_error("Failed to lock file");
    }

    // Initialize the custom buffer and set it as the stream buffer
    buffer_ = new FileInputBuffer(file_);
    this->rdbuf(buffer_);
  }

  ~ExclusiveInputFileStream() {
    flock(fileno(file_), LOCK_UN);
    fclose(file_);
    delete buffer_;
  }

 private:
  FILE* file_;
  FileInputBuffer* buffer_;
};