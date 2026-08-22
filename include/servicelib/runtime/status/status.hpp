/*
 * Built-in service topology status model.
 *
 * The HTTP adapter is intentionally separate from this file. ServiceApp
 * publishes a provider here; userver handlers only render immutable response
 * strings and therefore do not need to know the generated service type.
 */
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include <servicelib/runtime/config/config.hpp>
#include <servicelib/runtime/topology.hpp>

namespace servicelib::status {

class Provider {
 public:
  virtual ~Provider() = default;
  virtual std::string networkDataJson() const = 0;
  virtual std::string graphYaml() const = 0;
};

class Registry final {
 public:
  static void Register(Provider& provider) {
    Provider* expected = nullptr;
    if (!provider_.compare_exchange_strong(
            expected, &provider, std::memory_order_release,
            std::memory_order_relaxed)) {
      throw std::logic_error("a servicelib status provider is already active");
    }
  }

  static void Unregister(const Provider& provider) noexcept {
    Provider* expected = const_cast<Provider*>(&provider);
    provider_.compare_exchange_strong(
        expected, nullptr, std::memory_order_acq_rel,
        std::memory_order_relaxed);
  }

  static Provider* Get() noexcept {
    return provider_.load(std::memory_order_acquire);
  }

 private:
  inline static std::atomic<Provider*> provider_{nullptr};
};

inline std::string_view TransformationName(
    api::TransformationType type) noexcept {
  switch (type) {
    case api::TransformationType::kInput:
      return "INPUT";
    case api::TransformationType::kMap:
      return "MAP";
    case api::TransformationType::kFilter:
      return "FILTER";
    case api::TransformationType::kJoin:
      return "JOIN";
    case api::TransformationType::kMultiJoin:
      return "MULTIJOIN";
    case api::TransformationType::kProcess:
      return "PROCESS";
    case api::TransformationType::kFlatMap:
      return "FLATMAP";
    case api::TransformationType::kFlatMapIterable:
      return "FLATMAPITERABLE";
    case api::TransformationType::kKeyBy:
      return "KEYBY";
    case api::TransformationType::kMerge:
      return "MERGE";
    case api::TransformationType::kSplit:
      return "SPLIT";
    case api::TransformationType::kCase:
      return "CASE";
    case api::TransformationType::kSink:
      return "SINK";
    case api::TransformationType::kCycleLink:
      return "CYCLELINK";
    case api::TransformationType::kError:
      return "ERROR";
    case api::TransformationType::kDelay:
      return "DELAY";
    case api::TransformationType::kWhen:
      return "WHEN";
    case api::TransformationType::kUndefined:
      return "UNDEFINED";
  }
  return "UNDEFINED";
}

inline std::string EscapeSvg(std::string_view value) {
  std::string result;
  result.reserve(value.size() * 2);
  for (const char c : value) {
    switch (c) {
      case ' ':
        result += "%20";
        break;
      case '<':
        result += "%3C";
        break;
      case '>':
        result += "%3E";
        break;
      case '#':
        result += "%23";
        break;
      case '"':
        result += "%22";
        break;
      case '{':
        result += "%7B";
        break;
      case '}':
        result += "%7D";
        break;
      default:
        result += c;
    }
  }
  return result;
}

inline constexpr std::string_view kApiIcon =
    "M7 7H5A2 2 0 0 0 3 9V17H5V13H7V17H9V9A2 2 0 0 0 7 7M7 "
    "11H5V9H7M14 7H10V17H12V13H14A2 2 0 0 0 16 11V9A2 2 0 0 0 14 "
    "7M14 11H12V9H14M20 9V15H21V17H17V15H18V9H17V7H21V9Z";

inline constexpr std::string_view kCallMadeIcon =
    "M9,5V7H15.59L4,18.59L5.41,20L17,8.41V15H19V5";

inline std::string_view IconPath(api::TransformationType type) noexcept {
  constexpr std::string_view kDatabaseArrowRight =
      "M4 7C4 4.79 7.58 3 12 3S20 4.79 20 7 16.42 11 12 11 4 9.21 4 "
      "7M19.72 13.05C19.9 12.71 20 12.36 20 12V9C20 11.21 16.42 13 12 "
      "13S4 11.21 4 9V12C4 14.21 7.58 16 12 16C12.65 16 13.28 15.96 "
      "13.88 15.89C14.93 14.16 16.83 13 19 13C19.24 13 19.5 13 19.72 "
      "13.05M13.1 17.96C12.74 18 12.37 18 12 18C7.58 18 4 16.21 4 "
      "14V17C4 19.21 7.58 21 12 21C12.46 21 12.9 21 13.33 20.94C13.12 "
      "20.33 13 19.68 13 19C13 18.64 13.04 18.3 13.1 17.96M23 19L20 "
      "16V18H16V20H20V22L23 19Z";
  constexpr std::string_view kArrowLeftRight =
      "M6.45,17.45L1,12L6.45,6.55L7.86,7.96L4.83,11H19.17L16.14,7.96"
      "L17.55,6.55L23,12L17.55,17.45L16.14,16.04L19.17,13H4.83"
      "L7.86,16.04L6.45,17.45Z";
  constexpr std::string_view kFilter =
      "M14,12V19.88C14.04,20.18 13.94,20.5 13.71,20.71C13.32,21.1 "
      "12.69,21.1 12.3,20.71L10.29,18.7C10.06,18.47 9.96,18.16 "
      "10,17.87V12H9.97L4.21,4.62C3.87,4.19 3.95,3.56 4.38,3.22C4.57,"
      "3.08 4.78,3 5,3V3H19V3C19.22,3 19.43,3.08 19.62,3.22C20.05,3.56 "
      "20.13,4.19 19.79,4.62L14.03,12H14Z";
  constexpr std::string_view kCallMerge =
      "M17,20.41L18.41,19L15,15.59L13.59,17M7.5,8H11V13.59L5.59,19"
      "L7,20.41L13,14.41V8H16.5L12,3.5";
  constexpr std::string_view kFunction =
      "M15.6,5.29C14.5,5.19 13.53,6 13.43,7.11L13.18,10H16V12H13"
      "L12.56,17.07C12.37,19.27 10.43,20.9 8.23,20.7C6.92,20.59 "
      "5.82,19.86 5.17,18.83L6.67,17.33C6.91,18.07 7.57,18.64 8.4,"
      "18.71C9.5,18.81 10.47,18 10.57,16.89L11,12H8V10H11.17L11.44,"
      "6.93C11.63,4.73 13.57,3.1 15.77,3.3C17.08,3.41 18.18,4.14 "
      "18.83,5.17L17.33,6.67C17.09,5.93 16.43,5.36 15.6,5.29Z";
  constexpr std::string_view kTransit =
      "M18,11H14.82C14.4,9.84 13.3,9 12,9C10.7,9 9.6,9.84 9.18,11H6"
      "C5.67,11 4,10.9 4,9V8C4,6.17 5.54,6 6,6H16.18C16.6,7.16 17.7,"
      "8 19,8A3,3 0 0,0 22,5A3,3 0 0,0 19,2C17.7,2 16.6,2.84 "
      "16.18,4H6C4.39,4 2,5.06 2,8V9C2,11.94 4.39,13 6,13H9.18C9.6,"
      "14.16 10.7,15 12,15C13.3,15 14.4,14.16 14.82,13H18C18.33,13 "
      "20,13.1 20,15V16C20,17.83 18.46,18 18,18H7.82C7.4,16.84 6.3,"
      "16 5,16A3,3 0 0,0 2,19A3,3 0 0,0 5,22C6.3,22 7.4,21.16 "
      "7.82,20H18C19.61,20 22,18.93 22,16V15C22,12.07 19.61,11 18,"
      "11M19,4A1,1 0 0,1 20,5A1,1 0 0,1 19,6A1,1 0 0,1 18,5A1,1 "
      "0 0,1 19,4M5,20A1,1 0 0,1 4,19A1,1 0 0,1 5,18A1,1 0 0,1 "
      "6,19A1,1 0 0,1 5,20Z";
  constexpr std::string_view kKey =
      "M7 14C5.9 14 5 13.1 5 12S5.9 10 7 10 9 10.9 9 12 8.1 14 7 "
      "14M12.6 10C11.8 7.7 9.6 6 7 6C3.7 6 1 8.7 1 12S3.7 18 7 "
      "18C9.6 18 11.8 16.3 12.6 14H16V18H20V14H23V10H12.6Z";
  constexpr std::string_view kMerge =
      "M8 17L12 13H15.2C15.6 14.2 16.7 15 18 15C19.7 15 21 13.7 21 "
      "12S19.7 9 18 9C16.7 9 15.6 9.8 15.2 11H12L8 7V3H3V8H6L10.2 "
      "12L6 16H3V21H8V17Z";
  constexpr std::string_view kCallSplit =
      "M14,4L16.29,6.29L13.41,9.17L14.83,10.59L17.71,7.71L20,10V4"
      "M10,4H4V10L6.29,7.71L11,12.41V20H13V11.59L7.71,6.29";
  constexpr std::string_view kSourceFork =
      "M6,2A3,3 0 0,1 9,5C9,6.28 8.19,7.38 7.06,7.81C7.15,8.27 7.39,"
      "8.83 8,9.63C9,10.92 11,12.83 12,14.17C13,12.83 15,10.92 16,"
      "9.63C16.61,8.83 16.85,8.27 16.94,7.81C15.81,7.38 15,6.28 15,"
      "5A3,3 0 0,1 18,2A3,3 0 0,1 21,5C21,6.32 20.14,7.45 18.95,"
      "7.85C18.87,8.37 18.64,9 18,9.83C17,11.17 15,13.08 14,14.38"
      "C13.39,15.17 13.15,15.73 13.06,16.19C14.19,16.62 15,17.72 15,"
      "19A3,3 0 0,1 12,22A3,3 0 0,1 9,19C9,17.72 9.81,16.62 10.94,"
      "16.19C10.85,15.73 10.61,15.17 10,14.38C9,13.08 7,11.17 6,9.83"
      "C5.36,9 5.13,8.37 5.05,7.85C3.86,7.45 3,6.32 3,5A3,3 0 0,1 "
      "6,2M6,4A1,1 0 0,0 5,5A1,1 0 0,0 6,6A1,1 0 0,0 7,5A1,1 0 "
      "0,0 6,4M18,4A1,1 0 0,0 17,5A1,1 0 0,0 18,6A1,1 0 0,0 19,"
      "5A1,1 0 0,0 18,4M12,18A1,1 0 0,0 11,19A1,1 0 0,0 12,20A1,"
      "1 0 0,0 13,19A1,1 0 0,0 12,18Z";
  constexpr std::string_view kDatabaseArrowLeft =
      "M4 7C4 4.79 7.58 3 12 3S20 4.79 20 7 16.42 11 12 11 4 9.21 4 "
      "7M19.72 13.05C19.9 12.71 20 12.36 20 12V9C20 11.21 16.42 13 "
      "12 13S4 11.21 4 9V12C4 14.21 7.58 16 12 16C12.65 16 13.28 "
      "15.96 13.88 15.89C14.93 14.16 16.83 13 19 13C19.24 13 19.5 "
      "13 19.72 13.05M13.1 17.96C12.74 18 12.37 18 12 18C7.58 18 4 "
      "16.21 4 14V17C4 19.21 7.58 21 12 21C12.46 21 12.9 21 13.33 "
      "20.94C13.12 20.33 13 19.68 13 19C13 18.64 13.04 18.3 13.1 "
      "17.96M18 18V16L15 19L18 22V20H22V18H18Z";
  constexpr std::string_view kSync =
      "M12,18A6,6 0 0,1 6,12C6,11 6.25,10.03 6.7,9.2L5.24,7.74C4.46,"
      "8.97 4,10.43 4,12A8,8 0 0,0 12,20V23L16,19L12,15M12,4V1L8,"
      "5L12,9V6A6,6 0 0,1 18,12C18,13 17.75,13.97 17.3,14.8L18.76,"
      "16.26C19.54,15.03 20,13.57 20,12A8,8 0 0,0 12,4Z";
  constexpr std::string_view kAlert =
      "M13,13H11V7H13M13,17H11V15H13M12,2A10,10 0 0,0 2,12A10,10 "
      "0 0,0 12,22A10,10 0 0,0 22,12A10,10 0 0,0 12,2Z";
  constexpr std::string_view kTimer =
      "M19.03 7.39L20.45 5.97C20 5.46 19.55 5 19.04 4.56L17.62 6C16.07 "
      "4.74 14.12 4 12 4C7.03 4 3 8.03 3 13S7.03 22 12 22C17 22 21 "
      "17.97 21 13C21 10.88 20.26 8.93 19.03 7.39M13 14H11V7H13V14"
      "M15 1H9V3H15V1Z";
  constexpr std::string_view kSourceBranch =
      "M13,14C9.64,14 8.54,15.35 8.18,16.24C9.25,16.7 10,17.76 10,"
      "19A3,3 0 0,1 7,22A3,3 0 0,1 4,19C4,17.69 4.83,16.58 6,16.17"
      "V7.83C4.83,7.42 4,6.31 4,5A3,3 0 0,1 7,2A3,3 0 0,1 10,5C10,"
      "6.31 9.17,7.42 8,7.83V13.12C8.88,12.47 10.16,12 12,12C14.67,"
      "12 15.56,10.66 15.85,9.77C14.77,9.32 14,8.25 14,7A3,3 0 0,1 "
      "17,4A3,3 0 0,1 20,7C20,8.34 19.12,9.5 17.91,9.86C17.65,11.29 "
      "16.68,14 13,14M7,18A1,1 0 0,0 6,19A1,1 0 0,0 7,20A1,1 0 0,"
      "0 8,19A1,1 0 0,0 7,18M7,4A1,1 0 0,0 6,5A1,1 0 0,0 7,6A1,"
      "1 0 0,0 8,5A1,1 0 0,0 7,4M17,6A1,1 0 0,0 16,7A1,1 0 0,0 "
      "17,8A1,1 0 0,0 18,7A1,1 0 0,0 17,6Z";

  switch (type) {
    case api::TransformationType::kInput:
      return kDatabaseArrowRight;
    case api::TransformationType::kMap:
      return kArrowLeftRight;
    case api::TransformationType::kFilter:
      return kFilter;
    case api::TransformationType::kJoin:
    case api::TransformationType::kMultiJoin:
      return kCallMerge;
    case api::TransformationType::kProcess:
      return kFunction;
    case api::TransformationType::kFlatMap:
    case api::TransformationType::kFlatMapIterable:
      return kTransit;
    case api::TransformationType::kKeyBy:
      return kKey;
    case api::TransformationType::kMerge:
      return kMerge;
    case api::TransformationType::kSplit:
      return kCallSplit;
    case api::TransformationType::kCase:
      return kSourceFork;
    case api::TransformationType::kSink:
      return kDatabaseArrowLeft;
    case api::TransformationType::kCycleLink:
      return kSync;
    case api::TransformationType::kError:
      return kAlert;
    case api::TransformationType::kDelay:
      return kTimer;
    case api::TransformationType::kWhen:
      return kSourceBranch;
    case api::TransformationType::kUndefined:
      return kFunction;
  }
  return kFunction;
}

inline bool StreamIconIsApi(const config::RuntimeConfig& runtime,
                            const config::StreamConfigRef& stream) noexcept;

inline std::string_view StreamIconPath(
    const config::RuntimeConfig& runtime,
    const config::StreamConfigRef& stream) noexcept {
  if (StreamIconIsApi(runtime, stream)) {
    return stream.GetType() == api::TransformationType::kSink
               ? kCallMadeIcon
               : kApiIcon;
  }
  return IconPath(stream.GetType());
}

inline bool StreamIconIsApi(const config::RuntimeConfig& runtime,
                            const config::StreamConfigRef& stream) noexcept {
  int endpoint_id = 0;
  if (const auto* input = stream.As<config::InputStreamConfig>()) {
    endpoint_id = input->idEndpoint;
  } else if (const auto* sink = stream.As<config::SinkStreamConfig>()) {
    endpoint_id = sink->idEndpoint;
  }
  if (endpoint_id == 0) return false;
  const auto endpoint = runtime.GetEndpointConfigByID(endpoint_id);
  if (!endpoint) return false;
  const auto connector =
      runtime.GetDataConnectorByID(endpoint->GetIdDataConnector());
  return connector &&
         (connector->GetType() == api::DataConnectorType::kHTTP ||
          connector->GetType() == api::DataConnectorType::kGRPC);
}

inline std::string NodeImage(std::string_view icon,
                             std::string_view color, bool selected,
                             bool round = false) {
  const std::string background_radius = round ? "30" : "10";
  const std::string border_radius = round ? "28" : "9";
  std::string svg =
      std::string{"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"60\" "
                  "height=\"60\"><rect width=\"60\" height=\"60\" rx=\""} +
      background_radius + "\" fill=\"" +
      std::string(color) +
      "\"/><svg x=\"10\" y=\"10\" width=\"40\" height=\"40\" "
      "viewBox=\"0 0 24 24\"><path d=\"" +
      std::string(icon) + "\" fill=\"white\"/></svg>";
  if (selected) {
    svg +=
        "<rect x=\"2\" y=\"2\" width=\"56\" height=\"56\" rx=\"" +
        border_radius +
        "\" fill=\"none\" stroke=\"#00FF80\" stroke-width=\"4\"/>";
  }
  svg += "</svg>";
  return "data:image/svg+xml;charset=utf-8," + EscapeSvg(svg);
}

using CountFunction = std::function<std::int64_t(config::LinkID)>;

namespace detail {

inline std::string JsonString(std::string_view value) {
  std::ostringstream out;
  out << '"';
  constexpr char hex[] = "0123456789abcdef";
  for (const unsigned char c : value) {
    switch (c) {
      case '"':
        out << "\\\"";
        break;
      case '\\':
        out << "\\\\";
        break;
      case '\b':
        out << "\\b";
        break;
      case '\f':
        out << "\\f";
        break;
      case '\n':
        out << "\\n";
        break;
      case '\r':
        out << "\\r";
        break;
      case '\t':
        out << "\\t";
        break;
      default:
        if (c < 0x20) {
          out << "\\u00" << hex[c >> 4U] << hex[c & 0x0FU];
        } else {
          out << static_cast<char>(c);
        }
    }
  }
  out << '"';
  return out.str();
}

}  // namespace detail

inline std::string MakeNetworkDataJson(const config::RuntimeConfig& runtime,
                                       const CountFunction& count) {
  const auto& config = runtime.GetConfig();
  std::ostringstream nodes;
  std::ostringstream edges;
  bool firstNode = true;
  bool firstEdge = true;

  for (const auto& stream : config.GetStreams()) {
    const auto* service = runtime.GetServiceConfigByID(stream.GetIdService());
    const std::string serviceName = service ? service->name : std::string{};
    const std::string color =
        service && !service->color.empty() ? service->color : "#4A90D9";

    const auto icon = StreamIconPath(runtime, stream);
    const bool round = StreamIconIsApi(runtime, stream);
    if (!firstNode) nodes << ',';
    firstNode = false;
    nodes << "{\"id\":" << stream.GetID() << ",\"label\":"
          << detail::JsonString(stream.GetName() + "(" +
                        std::string(TransformationName(stream.GetType())) +
                        ")\n[" + serviceName + "]")
          << ",\"shape\":\"image\",\"image\":{\"unselected\":"
          << detail::JsonString(NodeImage(icon, color, false, round))
          << ",\"selected\":" << detail::JsonString(NodeImage(icon, color, true, round))
          << "},\"size\":30,\"opacity\":1.0,\"x\":"
          << stream.GetXPos() << ",\"y\":" << stream.GetYPos()
          << ",\"color\":{\"border\":\"transparent\",\"highlight\":{"
             "\"border\":\"transparent\"}}}";

    std::unordered_set<int> sources;
    if (stream.GetIdSource() != 0) sources.insert(stream.GetIdSource());
    for (const int source : stream.GetIdSources()) {
      if (source != 0) sources.insert(source);
    }
    for (const int source : sources) {
      std::string label =
          "calls: " + std::to_string(count({source, stream.GetID()}));
      if ((stream.GetType() == api::TransformationType::kJoin ||
           stream.GetType() == api::TransformationType::kMultiJoin) &&
          stream.GetIdSource() != 0) {
        label += source == stream.GetIdSource() ? " (L)" : " (R)";
      }
      if (!firstEdge) edges << ',';
      firstEdge = false;
      edges << "{\"from\":" << source << ",\"to\":" << stream.GetID()
            << ",\"arrows\":\"to\",\"length\":200,\"label\":"
            << detail::JsonString(label)
            << ",\"color\":{\"opacity\":1.0,\"color\":\"#0050FF\"}}";
    }
  }

  return "{\"nodes\":[" + nodes.str() + "],\"edges\":[" + edges.str() +
         "]}";
}

inline std::string MakeNetworkDataJson(
    const config::RuntimeConfig& runtime,
    const StatusTopologyPrinter& topology, const CountFunction& count) {
  std::ostringstream nodes;
  std::ostringstream edges;
  bool firstNode = true;
  bool firstEdge = true;
  std::unordered_set<std::int64_t> emittedNodes;
  std::unordered_set<std::string> emittedEdges;
  for (const auto& topologyNode : topology.nodes) {
    const auto stream = runtime.GetStreamConfigByID(
        static_cast<int>(topologyNode.configId));
    if (!stream) {
      throw std::runtime_error("status topology references missing stream " +
                               std::to_string(topologyNode.configId));
    }
    const auto* service =
        runtime.GetServiceConfigByID(stream->GetIdService());
    const std::string serviceName = service ? service->name : std::string{};
    const std::string color =
        service && !service->color.empty() ? service->color : "#4A90D9";
    const bool isError = static_cast<std::int64_t>(topologyNode.id) < 0;
    const auto nodeId = isError
                            ? -static_cast<std::int64_t>(topologyNode.configId)
                            : static_cast<std::int64_t>(topologyNode.configId);
    if (!emittedNodes.insert(nodeId).second) continue;
    const auto icon = isError ? IconPath(api::TransformationType::kError)
                              : StreamIconPath(runtime, *stream);
    const bool round = !isError && StreamIconIsApi(runtime, *stream);

    if (!firstNode) nodes << ',';
    firstNode = false;
    nodes << "{\"id\":" << nodeId
          << ",\"label\":"
          << detail::JsonString(stream->GetName() +
                                (isError ? " Error" : "") + "(" +
                                std::string(TransformationName(
                                    isError ? api::TransformationType::kError
                                            : stream->GetType())) +
                                ")\n[" + serviceName + "]")
          << ",\"shape\":\"image\",\"image\":{\"unselected\":"
          << detail::JsonString(NodeImage(icon, color, false, round))
          << ",\"selected\":"
          << detail::JsonString(NodeImage(icon, color, true, round))
          << "},\"size\":30,\"opacity\":1.0,\"x\":"
          << (isError ? 0.0 : stream->GetXPos()) << ",\"y\":"
          << (isError ? 0.0 : stream->GetYPos())
          << ",\"color\":{\"border\":\"transparent\",\"highlight\":{"
             "\"border\":\"transparent\"}}}";
  }

  for (const auto& topologyEdge : topology.edges) {
    const auto source = runtime.GetStreamConfigByID(
        static_cast<int>(topologyEdge.from.configId));
    const auto target = runtime.GetStreamConfigByID(
        static_cast<int>(topologyEdge.to.configId));
    if (!source || !target) {
      throw std::runtime_error("status topology edge references missing stream");
    }
    const bool leavesError =
        static_cast<std::int64_t>(topologyEdge.from.id) < 0;
    const bool entersError =
        static_cast<std::int64_t>(topologyEdge.to.id) < 0;
    const auto fromId = leavesError
                            ? -static_cast<std::int64_t>(
                                  topologyEdge.from.configId)
                            : static_cast<std::int64_t>(
                                  topologyEdge.from.configId);
    const auto toId = entersError
                          ? -static_cast<std::int64_t>(
                                topologyEdge.to.configId)
                          : static_cast<std::int64_t>(topologyEdge.to.configId);
    if (fromId == toId) continue;
    if (!emittedEdges.insert(std::to_string(fromId) + ":" +
                             std::to_string(toId)).second) {
      continue;
    }
    std::string label = topologyEdge.from.typeName;
    if (!entersError) {
      const auto calls = leavesError
                             ? topologyEdge.from.emittedCount
                             : count({static_cast<int>(
                                          topologyEdge.from.configId),
                                      static_cast<int>(
                                          topologyEdge.to.configId)});
      label += "\ncalls: " + std::to_string(calls);
    }
    if ((target->GetType() == api::TransformationType::kJoin ||
         target->GetType() == api::TransformationType::kMultiJoin) &&
        target->GetIdSource() != 0) {
      label += topologyEdge.from.configId ==
                       static_cast<std::size_t>(target->GetIdSource())
                   ? " (L)"
                   : " (R)";
    }
    if (!firstEdge) edges << ',';
    firstEdge = false;
    edges << "{\"from\":" << fromId << ",\"to\":" << toId
          << ",\"arrows\":\"to\",\"length\":200,\"label\":"
          << detail::JsonString(label)
          << ",\"color\":{\"opacity\":1.0,\"color\":\""
          << (entersError ? "#FF3030" : "#0050FF") << "\"}}";
  }

  return "{\"nodes\":[" + nodes.str() + "],\"edges\":[" + edges.str() +
         "]}";
}

inline std::string YamlString(std::string_view value) {
  std::string result{"\""};
  for (const char c : value) {
    if (c == '\\' || c == '"') result += '\\';
    if (c == '\n') {
      result += "\\n";
    } else {
      result += c;
    }
  }
  result += '"';
  return result;
}

inline void AppendCallSemanticsYaml(
    std::ostringstream& out,
    const config::CallSemanticsGroup& semantics,
    std::string_view indent) {
  if (semantics.functionCall.has_value()) {
    out << indent << "callSemantics: FunctionCall\n";
    if (semantics.functionCall->async) {
      out << indent << "async: true\n";
    }
  } else if (semantics.taskPool.has_value()) {
    out << indent << "callSemantics: TaskPool\n"
        << indent << "poolName: "
        << YamlString(semantics.taskPool->poolName) << "\n";
  } else if (semantics.priorityTaskPool.has_value()) {
    out << indent << "callSemantics: PriorityTaskPool\n"
        << indent << "poolName: "
        << YamlString(semantics.priorityTaskPool->poolName) << "\n"
        << indent << "priority: " << semantics.priorityTaskPool->priority
        << "\n";
  } else if (semantics.parallelCall.has_value()) {
    out << indent << "callSemantics: ParallelCall\n";
  }
}

inline std::string MakeGraphYaml(const config::RuntimeConfig& runtime) {
  const auto& config = runtime.GetConfig();
  std::ostringstream out;
  out << "services:\n";
  for (const auto* service : config.GetServices()) {
    out << "  - id: " << service->id << "\n"
        << "    name: " << YamlString(service->name) << "\n"
        << "    color: " << YamlString(service->color) << "\n";
  }
  out << "streams:\n";
  for (const auto& stream : config.GetStreams()) {
    out << "  - id: " << stream.GetID() << "\n"
        << "    name: " << YamlString(stream.GetName()) << "\n"
        << "    type: " << TransformationName(stream.GetType()) << "\n"
        << "    idService: " << stream.GetIdService() << "\n"
        << "    xPos: " << stream.GetXPos() << "\n"
        << "    yPos: " << stream.GetYPos() << "\n";
    if (stream.GetIdSource() != 0) {
      out << "    idSource: " << stream.GetIdSource() << "\n";
    }
    if (!stream.GetIdSources().empty()) {
      out << "    idSources: [";
      for (std::size_t i = 0; i < stream.GetIdSources().size(); ++i) {
        if (i != 0) out << ", ";
        out << stream.GetIdSources()[i];
      }
      out << "]\n";
    }
  }
  out << "pools:\n";
  for (const auto* pool : config.GetPools()) {
    if (!pool) continue;
    out << "  - name: " << YamlString(pool->name) << "\n"
        << "    executorsCount: " << pool->executorsCount << "\n";
  }
  out << "links:\n";
  for (const auto* link : config.GetLinks()) {
    if (!link) continue;
    out << "  - from: " << link->from << "\n"
        << "    to: " << link->to << "\n";
    if (link->callSemantics.has_value()) {
      AppendCallSemanticsYaml(out, *link->callSemantics, "    ");
    }
  }
  return out.str();
}

}  // namespace servicelib::status
