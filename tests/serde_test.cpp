#include <servicelib/runtime/serde/serdeimpl.hpp>

#include <any>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using servicelib::serde::SerdeData;
using servicelib::serde::SerdeError;
using servicelib::serde::SerdeLimits;

void Require(bool condition, const char* message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

template <typename Exception, typename Function>
void RequireThrows(Function&& function, const char* message) {
  try {
    std::forward<Function>(function)();
  } catch (const Exception&) {
    return;
  }
  throw std::runtime_error(message);
}

SerdeData Bytes(std::initializer_list<std::uint8_t> values) {
  SerdeData result;
  result.reserve(values.size());
  for (const auto value : values) {
    result.push_back(static_cast<std::byte>(value));
  }
  return result;
}

void Append(SerdeData& destination, const SerdeData& source) {
  destination.insert(destination.end(), source.begin(), source.end());
}

SerdeData Frame(const SerdeData& payload) {
  SerdeData result;
  servicelib::serde::detail::put_size(result, payload.size());
  Append(result, payload);
  return result;
}

template <typename T, typename Serde>
void RequireRoundTrip(const Serde& serde, const T& value) {
  Require(serde.Deserialize(serde.Serialize(value)) == value,
          "serde round trip mismatch");
}

void PrimitiveWireFormatMatchesGo() {
  const servicelib::serde::BoolSerde boolSerde;
  Require(boolSerde.Serialize(false) == Bytes({0x00}), "false wire format");
  Require(boolSerde.Serialize(true) == Bytes({0x01}), "true wire format");
  Require(boolSerde.Deserialize(Bytes({0xff})), "non-zero bool decode");

  const servicelib::serde::Int16Serde int16Serde;
  Require(int16Serde.Serialize(std::numeric_limits<std::int16_t>::min()) ==
              Bytes({0x00, 0x00}),
          "int16 min wire format");
  Require(int16Serde.Serialize(-1) == Bytes({0x7f, 0xff}),
          "int16 negative wire format");
  Require(int16Serde.Serialize(0) == Bytes({0x80, 0x00}),
          "int16 zero wire format");
  Require(int16Serde.Serialize(std::numeric_limits<std::int16_t>::max()) ==
              Bytes({0xff, 0xff}),
          "int16 max wire format");

  const servicelib::serde::Int32Serde int32Serde;
  Require(int32Serde.Serialize(-1) == Bytes({0x7f, 0xff, 0xff, 0xff}),
          "int32 negative wire format");
  Require(int32Serde.Serialize(0) == Bytes({0x80, 0x00, 0x00, 0x00}),
          "int32 zero wire format");

  const servicelib::serde::Int64Serde int64Serde;
  Require(int64Serde.Serialize(-1) ==
              Bytes({0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
          "int64 negative wire format");
  Require(int64Serde.Serialize(0) ==
              Bytes({0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}),
          "int64 zero wire format");

  const servicelib::serde::UInt32Serde uint32Serde;
  Require(uint32Serde.Serialize(0x01020304u) ==
              Bytes({0x01, 0x02, 0x03, 0x04}),
          "uint32 wire format");

  const servicelib::serde::RuneSerde runeSerde;
  Require(runeSerde.Serialize(U'Ж') == Bytes({0x00, 0x00, 0x04, 0x16}),
          "rune wire format");
}

void FloatingPointRoundTripPreservesBits() {
  const servicelib::serde::Float32Serde floatSerde;
  const float floatNan = std::bit_cast<float>(std::uint32_t{0x7fc01234u});
  const float decodedFloat =
      floatSerde.Deserialize(floatSerde.Serialize(floatNan));
  Require(std::bit_cast<std::uint32_t>(decodedFloat) == 0x7fc01234u,
          "float NaN payload");
  Require(floatSerde.Serialize(1.0f) == Bytes({0x3f, 0x80, 0x00, 0x00}),
          "float wire format");

  const servicelib::serde::Float64Serde doubleSerde;
  const double doubleNan =
      std::bit_cast<double>(std::uint64_t{0x7ff8000000001234ull});
  const double decodedDouble =
      doubleSerde.Deserialize(doubleSerde.Serialize(doubleNan));
  Require(std::bit_cast<std::uint64_t>(decodedDouble) ==
              0x7ff8000000001234ull,
          "double NaN payload");
  RequireRoundTrip(doubleSerde, -0.0);
}

void StringBytesAndSerializeToUseLengthPrefixAndAppend() {
  const servicelib::serde::StringSerde stringSerde;
  const std::string stringValue{"A\0B", 3};
  Require(stringSerde.Serialize(stringValue) ==
              Bytes({0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 'A',
                     0x00, 'B'}),
          "string wire format");
  RequireRoundTrip(stringSerde, stringValue);

  const servicelib::serde::UInt8ArraySerde bytesSerde;
  const std::vector<std::uint8_t> bytesValue{0x00, 0x7f, 0xff};
  RequireRoundTrip(bytesSerde, bytesValue);

  SerdeData output = Bytes({0xaa, 0xbb});
  stringSerde.SerializeTo(output, "x");
  Require(output == Bytes({0xaa, 0xbb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                           0x00, 0x01, 'x'}),
          "SerializeTo append semantics");
}

void FixedAndFramedArraysRoundTrip() {
  const servicelib::serde::Int16ArraySerde fixedSerde;
  const std::vector<std::int16_t> fixed{-32768, -1, 0, 32767};
  Require(fixedSerde.Serialize(fixed) ==
              Bytes({0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
                     0x00, 0x7f, 0xff, 0x80, 0x00, 0xff, 0xff}),
          "fixed array wire format");
  RequireRoundTrip(fixedSerde, fixed);

  const auto stringSerde = std::make_shared<servicelib::serde::StringSerde>();
  const servicelib::serde::ArraySerde<std::string> framedSerde{stringSerde};
  const std::vector<std::string> framed{"one", "", "three"};
  RequireRoundTrip(framedSerde, framed);

  const auto defaultSerde =
      servicelib::serde::MakeDefaultSerde<std::vector<std::string>>();
  Require(!defaultSerde->IsStub(), "default array serde must not be stub");
  RequireRoundTrip(*defaultSerde, framed);
}

void MapRoundTripAndMalformedCountMismatch() {
  using Map = std::unordered_map<std::string, std::int32_t>;
  const auto serde = servicelib::serde::MakeDefaultSerde<Map>();
  const Map value{{"alpha", 1}, {"beta", -2}, {"", 0}};
  RequireRoundTrip(*serde, value);

  const servicelib::serde::StringArraySerde keysSerde;
  const servicelib::serde::Int32ArraySerde valuesSerde;
  SerdeData malformed = Frame(keysSerde.Serialize({"a", "b"}));
  Append(malformed, Frame(valuesSerde.Serialize({1})));
  RequireThrows<SerdeError>([&] { static_cast<void>(serde->Deserialize(malformed)); },
                            "map count mismatch accepted");
}

void StreamWrappersAndKeyValueSerde() {
  auto valueSerde = std::make_shared<servicelib::serde::StringSerde>();
  auto streamSerde = servicelib::serde::MakeStreamSerde<std::string>(valueSerde);
  valueSerde.reset();
  Require(!streamSerde->IsKeyValue(), "value stream reported key/value");
  Require(streamSerde->Deserialize(streamSerde->Serialize("payload")) ==
              "payload",
          "stream serde round trip");
  Require(streamSerde->ValueSerializer() != nullptr,
          "stream value serializer missing");

  using KeyValue = std::pair<std::int32_t, std::string>;
  auto keyValueSerde =
      servicelib::serde::MakeStreamKeyValueSerde<std::int32_t, std::string,
                                                 KeyValue>(
          std::make_shared<servicelib::serde::Int32Serde>(),
          std::make_shared<servicelib::serde::StringSerde>());
  const KeyValue value{-7, "seven"};
  Require(keyValueSerde->IsKeyValue(), "key/value stream not reported");
  Require(keyValueSerde->Deserialize(keyValueSerde->Serialize(value)) == value,
          "key/value serde round trip");
  Require(keyValueSerde->DeserializeKeyValue(keyValueSerde->SerializeKey(value),
                                              keyValueSerde->SerializeValue(value)) ==
              value,
          "split key/value serde round trip");
}

void TypeErasureRejectsWrongObjectType() {
  const servicelib::serde::Int32Serde typedSerde;
  const servicelib::serde::Serializer& serializer = typedSerde;
  const auto encoded = serializer.SerializeObj(std::any{std::int32_t{42}});
  Require(std::any_cast<std::int32_t>(serializer.DeserializeObj(encoded)) == 42,
          "type-erased serde round trip");
  RequireThrows<std::invalid_argument>(
      [&] { static_cast<void>(serializer.SerializeObj(std::any{"wrong"})); },
      "wrong any type accepted");

  servicelib::serde::StubSerde<std::string> stub;
  Require(stub.IsStub(), "stub serde marker");
  RequireThrows<std::runtime_error>(
      [&] { static_cast<void>(stub.Serialize("value")); },
      "stub serialize succeeded");
  RequireThrows<std::runtime_error>(
      [&] { static_cast<void>(stub.Deserialize({})); },
      "stub deserialize succeeded");
  RequireThrows<std::invalid_argument>(
      [] { static_cast<void>(servicelib::serde::MakeStreamSerde<std::string>(nullptr)); },
      "null stream serde dependency accepted");
}

void LimitsAreEnforcedOnEncodeAndDecode() {
  const servicelib::serde::StringSerde stringSerde{
      SerdeLimits{.maxStringBytes = 3}};
  RequireThrows<SerdeError>(
      [&] { static_cast<void>(stringSerde.Serialize("four")); },
      "string encode limit ignored");

  const auto encodedString = servicelib::serde::StringSerde{}.Serialize("four");
  RequireThrows<SerdeError>(
      [&] { static_cast<void>(stringSerde.Deserialize(encodedString)); },
      "string decode limit ignored");

  const servicelib::serde::UInt8ArraySerde bytesSerde{
      SerdeLimits{.maxBytes = 2}};
  RequireThrows<SerdeError>(
      [&] { static_cast<void>(bytesSerde.Serialize({1, 2, 3})); },
      "byte limit ignored");

  const servicelib::serde::Int32ArraySerde arraySerde{
      SerdeLimits{.maxContainerElements = 1}};
  RequireThrows<SerdeError>(
      [&] { static_cast<void>(arraySerde.Serialize({1, 2})); },
      "container element limit ignored");

  const servicelib::serde::StringSerde totalLimited{
      SerdeLimits{.maxTotalBytes = 8}};
  RequireThrows<SerdeError>(
      [&] { static_cast<void>(totalLimited.Deserialize(encodedString)); },
      "total byte limit ignored");
}

void TruncatedFramesReportTheReadOffset() {
  const servicelib::serde::StringSerde stringSerde;
  const auto truncatedString =
      Bytes({0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 'a', 'b'});
  try {
    static_cast<void>(stringSerde.Deserialize(truncatedString));
    throw std::runtime_error("truncated string accepted");
  } catch (const SerdeError& error) {
    Require(error.offset() == 8, "wrong truncated-string read offset");
  }

  const servicelib::serde::Int32ArraySerde fixedSerde;
  RequireThrows<SerdeError>(
      [&] {
        static_cast<void>(fixedSerde.Deserialize(Bytes(
            {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x80, 0x00,
             0x00, 0x01})));
      },
      "truncated fixed array accepted");

  const servicelib::serde::ArraySerde<std::string> framedSerde{
      std::make_shared<servicelib::serde::StringSerde>()};
  RequireThrows<SerdeError>(
      [&] {
        static_cast<void>(framedSerde.Deserialize(Bytes(
            {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
             0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 'x'})));
      },
      "truncated framed array accepted");
}

void CompositeConstructorsRejectNullDependencies() {
  RequireThrows<std::invalid_argument>(
      [] {
        static_cast<void>(
            servicelib::serde::ArraySerde<std::string>{nullptr});
      },
      "array accepted null serde");

  using Map = std::unordered_map<std::string, std::int32_t>;
  RequireThrows<std::invalid_argument>(
      [] {
        static_cast<void>(
            servicelib::serde::MapSerde<std::string, std::int32_t, Map>{
                nullptr,
                servicelib::serde::MakeDefaultSerde<
                    std::vector<std::int32_t>>()});
      },
      "map accepted null serde");
}

}  // namespace

int main() {
  PrimitiveWireFormatMatchesGo();
  FloatingPointRoundTripPreservesBits();
  StringBytesAndSerializeToUseLengthPrefixAndAppend();
  FixedAndFramedArraysRoundTrip();
  MapRoundTripAndMalformedCountMismatch();
  StreamWrappersAndKeyValueSerde();
  TypeErasureRejectsWrongObjectType();
  LimitsAreEnforcedOnEncodeAndDecode();
  TruncatedFramesReportTheReadOffset();
  CompositeConstructorsRejectNullDependencies();
}
