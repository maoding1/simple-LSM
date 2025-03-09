#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include "NoCopyable.h"

class FileOperator : public NoCopyable {
 public:
  FileOperator() = default;
  ~FileOperator() override = default;
  virtual bool Open(const std::string &filename, bool create) = 0;
  virtual bool Create(const std::string &filename, const std::vector<uint8_t> &data) = 0;
  virtual void Close() = 0;
  virtual size_t Size() = 0;
  virtual bool Write(size_t offset, const void *data, size_t size) = 0;
  virtual std::vector<uint8_t> Read(size_t offset, size_t size) = 0;
  virtual bool Sync() = 0;
};

class StdFileOperator : public FileOperator {
 private:
  std::fstream file_;
  std::filesystem::path file_path_;

 public:
  StdFileOperator() = default;
  ~StdFileOperator() override {
    if (file_.is_open()) {
      Close();
    }
  }

  bool Open(const std::string &filename, bool create) override {
    file_path_ = std::filesystem::path(filename);
    if (create) {
      file_.open(file_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
    } else {
      file_.open(file_path_, std::ios::in | std::ios::out | std::ios::binary);
    }
    return file_.is_open();
  }

  bool Create(const std::string &filename, const std::vector<uint8_t> &data) override {
    if (!Open(filename, true)) {
      throw std::runtime_error("Failed to open file");
    }
    Write(0, data.data(), data.size());
    return true;
  }
  void Close() override {
    if (file_.is_open()) {
      Sync();
      file_.close();
    }
  }
  size_t Size() override {
    file_.seekg(0, std::ios::end);
    return static_cast<size_t>(file_.tellg());
  }

  bool Write(size_t offset, const void *data, size_t size) override {
    file_.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
    file_.write(reinterpret_cast<const char *>(data), static_cast<std::streamsize>(size));
    this->Sync();
    return true;
  }
  std::vector<uint8_t> Read(size_t offset, size_t size) override {
    std::vector<uint8_t> result(size);
    file_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    file_.read(reinterpret_cast<char *>(result.data()), static_cast<std::streamsize>(size));
    return result;
  }
  bool Sync() override {
    if (!file_.is_open()) {
      return false;
    }
    file_.flush();
    return file_.good();
  }
};

class FileObj {
  private: 
  std::unique_ptr<FileOperator> file_operator_;
  size_t size_{};
 public:
  FileObj() : file_operator_(std::make_unique<StdFileOperator>()){};
  ~FileObj() = default;

  FileObj(FileObj &&other) noexcept {
    file_operator_ = std::move(other.file_operator_);
    size_ = other.size_;
    other.size_ = 0;
  }

  FileObj &operator=(FileObj &&other) noexcept {
    if (this != &other) {
      file_operator_ = std::move(other.file_operator_);
      size_ = other.size_;
      other.size_ = 0;
    }
    return *this;
  }

  size_t Size() const { return file_operator_->Size(); }
  void SetSize(size_t size) { size_ = size; }
  static FileObj CreateAndWrite(const std::string &path, const std::vector<uint8_t> &data) {
    FileObj file;
    if (!file.file_operator_->Create(path, data)) {
      throw std::runtime_error("Failed to create file");
    }
    file.file_operator_->Sync();
    return file;
  }
  static FileObj Open(const std::string &path) {
    FileObj file;
    if (!file.file_operator_->Open(path, false)) {
      throw std::runtime_error("Failed to open file");
    }
    return file;
  }
  std::vector<uint8_t> Read(size_t offset, size_t size) {
    if (offset + size > Size()) {
      throw std::out_of_range("Read out of bound");
    }
    return file_operator_->Read(offset, size);
  }
};