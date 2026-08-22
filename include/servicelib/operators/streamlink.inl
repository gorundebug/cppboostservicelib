// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp.
// Go analog: operators/streamlink.go. Go delegates common Stream methods
// through streamLink; C++ expresses the same relationship through this
// inherited typed-stream base.

template <typename T, typename Consumer>
class TransformStream : public Stream<T, Consumer, _Context> {
 protected:
  TransformStream() noexcept = default;
  ~TransformStream() override = default;

  template <typename Downstream>
  explicit TransformStream(unique_ptr<Downstream> consumer) noexcept
      : Stream<T, Downstream, _Context>(std::move(consumer)) {}
};
