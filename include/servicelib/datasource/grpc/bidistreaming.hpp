#pragma once

#include <servicelib/datasource/grpc/common.hpp>

namespace servicelib::datasource::grpc {

template <typename Req, typename Res, typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class BidirectionalStreamingEndpoint final
    : public Endpoint<Req, Res, T, R, Handler, E> {
 public:
  using Output = typename Endpoint<Req, Res, T, R, Handler, E>::Output;
  using ErrorOutput =
      typename Endpoint<Req, Res, T, R, Handler, E>::ErrorOutput;

  BidirectionalStreamingEndpoint(IServiceEnvironment& environment,
                                 int endpointId, Handler handler, Output output,
                                 bool hasResult, ErrorOutput errorOutput = {})
      : BidirectionalStreamingEndpoint(
            environment, endpointId, 0, std::move(handler),
            std::move(output), hasResult, std::move(errorOutput)) {}

  BidirectionalStreamingEndpoint(IServiceEnvironment& environment,
                                 int endpointId, int streamConfigId,
                                 Handler handler, Output output, bool hasResult,
                                 ErrorOutput errorOutput = {})
      : Endpoint<Req, Res, T, R, Handler, E>(
            environment, endpointId, streamConfigId,
            api::GrpcMethodType::kBidirectionalStreaming, std::move(handler),
            std::move(output), hasResult, std::move(errorOutput)) {}

  template <typename ReaderWriter>
  void handle(MessageContext context, ReaderWriter& stream) {
    auto startedSpan = this->startTrace(context);
    auto sender = std::make_shared<Sender<Res>>(
        [&](Res response) { stream.Write(std::move(response)); },
        startedSpan.sharedSpan());
    auto request = this->begin(context, std::move(sender),
                               startedSpan.sharedSpan());
    const auto startedAt = this->metrics().requestStart();
    std::exception_ptr error;
    try {
      this->activate(request);
      Req value;
      std::int64_t messageCount = 0;
      while (stream.Read(value)) {
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
  }

  template <typename ReaderWriter>
  ::grpc::Status handle(::grpc::ServerContext& call,
                        ReaderWriter& stream) {
    handle(messageContext(call, this->tracingEnabled()), stream);
    return ::grpc::Status::OK;
  }
};

}  // namespace servicelib::datasource::grpc
