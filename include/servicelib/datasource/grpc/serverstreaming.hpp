#pragma once

#include <servicelib/datasource/grpc/common.hpp>

namespace servicelib::datasource::grpc {

template <typename Req, typename Res, typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class ServerStreamingEndpoint final
    : public Endpoint<Req, Res, T, R, Handler, E> {
 public:
  using Output = typename Endpoint<Req, Res, T, R, Handler, E>::Output;
  using ErrorOutput =
      typename Endpoint<Req, Res, T, R, Handler, E>::ErrorOutput;

  ServerStreamingEndpoint(IServiceEnvironment& environment, int endpointId,
                          Handler handler, Output output, bool hasResult,
                          ErrorOutput errorOutput = {})
      : ServerStreamingEndpoint(environment, endpointId, 0,
                                std::move(handler), std::move(output),
                                hasResult, std::move(errorOutput)) {}

  ServerStreamingEndpoint(IServiceEnvironment& environment, int endpointId,
                          int streamConfigId, Handler handler, Output output,
                          bool hasResult, ErrorOutput errorOutput = {})
      : Endpoint<Req, Res, T, R, Handler, E>(
            environment, endpointId, streamConfigId,
            api::GrpcMethodType::kServerStreaming, std::move(handler),
            std::move(output), hasResult, std::move(errorOutput)) {}

  template <typename Writer>
  void handle(MessageContext context, const Req& value, Writer& writer) {
    auto startedSpan = this->startTrace(context);
    auto sender = std::make_shared<Sender<Res>>(
        [&](Res response) { writer.Write(std::move(response)); },
        startedSpan.sharedSpan());
    auto request = this->begin(context, std::move(sender),
                               startedSpan.sharedSpan());
    const auto startedAt = this->metrics().requestStart();
    std::exception_ptr error;
    try {
      this->activate(request);
      this->consume(request, value);
      this->eof(request);
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

  template <typename Writer>
  ::grpc::Status handle(::grpc::ServerContext& call, Req&& value,
                        Writer& writer) {
    handle(messageContext(call, this->tracingEnabled()), value, writer);
    return ::grpc::Status::OK;
  }
};

}  // namespace servicelib::datasource::grpc
