class NoCopyable {
 protected:
  NoCopyable() = default;
  virtual ~NoCopyable() = default;

 public:
  NoCopyable(const NoCopyable &) = delete;
  NoCopyable &operator=(const NoCopyable &) = delete;
};
