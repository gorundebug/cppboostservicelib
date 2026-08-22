#pragma once

#include <servicelib/datasource/grpc/common.hpp>

namespace servicelib::datasource::grpc {

template <typename Req, typename Res, typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class ClientStreamingEndpoint final
    : public Endpoint<Req, Res, T, R, Handler, E> {
 public:
  using Request = typename Endpoint<Req, Res, T, R, Handler, E>::Request;
  using Output = typename Endpoint<Req, Res, T, R, Handler, E>::Output;
  using ErrorOutput =
      typename Endpoint<Req, Res, T, R, Handler, E>::ErrorOutput;

  ClientStreamingEndpoint(IServiceEnvironment& environment, int endpointId,
                          Handler handler, Output output, bool hasResult,
                          ErrorOutput errorOutput = {})
      : ClientStreamingEndpoint(environment, endpointId, 0,
                                std::move(handler), std::move(output),
                                hasResult, std::move(errorOutput)) {}

  ClientStreamingEndpoint(IServiceEnvironment& environment, int endpointId,
                          int streamConfigId, Handler handler, Output output,
                          bool hasResult, ErrorOutput errorOutput = {})
      : Endpoint<Req, Res, T, R, Handler, E>(
            environment, endpointId, streamConfigId,
            api::GrpcMethodType::kClientStreaming, std::move(handler),
            std::move(output), hasResult, std::move(errorOutput)) {}

  template <typename Reader>
  Res handle(MessageContext context, Reader& reader) {
    auto startedSpan = this->startTrace(context);
    std::optional<Res> response;
    auto requestSlot = std::make_shared<std::weak_ptr<Request>>();
    auto sender = std::make_shared<Sender<Res>>([&, requestSlot](Res result) {
      if (!response) response.emplace(std::move(result));
      if (auto request = requestSlot->lock()) {
        ResultContext<typename Handler::State, T, Res, R, E>{request}.done();
      }
    }, startedSpan.sharedSpan());
    auto request = this->begin(context, std::move(sender),
                               startedSpan.sharedSpan());
    *requestSlot = request;
    const auto startedAt = this->metrics().requestStart();
    std::exception_ptr error;
    try {
      this->activate(request);
      Req value;
      std::int64_t messageCount = 0;
      while (reader.Read(value)) {
        this->consume(request, value);
        ++messageCount;
      }
      this->eof(request, messageCount);
      this->waitDone(request);
    } catch (...) {
      error = std::current_exception();
      this->recordFailure(request, error);
    }
    try {
      this->finish(request, error);
    } catch (...) {
      if (!error) error = std::current_exception();
    }
    this->metrics().requestEnd(startedAt, error);
    if (error) std::rethrow_exception(error);
    return response ? std::move(*response) : Res{};
  }

  template <typename Reader>
  Res handle(::grpc::ServerContext& call, Reader& reader) {
    return handle(messageContext(call, this->tracingEnabled()), reader);
  }
};

}  // namespace servicelib::datasource::grpc
