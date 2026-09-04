// Tests for streaming request-body ("body sink") routes.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <future>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <ut/ut.hpp>

#include "glaze/net/http_server.hpp"

#if defined(GLZ_USING_BOOST_ASIO)
namespace asio
{
   using namespace boost::asio;
   using error_code = boost::system::error_code;
}
#endif

namespace
{
   using namespace ut;

   constexpr char test_host[] = "127.0.0.1";

   std::string build_head(std::string_view method, std::string_view route, uint16_t port, std::size_t content_length,
                          bool keep_alive)
   {
      std::ostringstream req;
      req << method << " " << route << " HTTP/1.1\r\n"
          << "Host: " << test_host << ":" << port << "\r\n"
          << "Content-Type: application/octet-stream\r\n"
          << "Content-Length: " << content_length << "\r\n"
          << "Connection: " << (keep_alive ? "keep-alive" : "close") << "\r\n\r\n";
      return req.str();
   }

   std::string body_of(const std::string& response)
   {
      const auto head_end = response.find("\r\n\r\n");
      return head_end == std::string::npos ? std::string{} : response.substr(head_end + 4);
   }

   // Read until the peer closes the connection (used for "Connection: close" responses).
   std::string read_until_close(asio::ip::tcp::socket& socket)
   {
      std::string resp;
      std::array<char, 4096> buf{};
      asio::error_code ec;
      for (;;) {
         const std::size_t n = socket.read_some(asio::buffer(buf), ec);
         if (n == 0 || ec) break;
         resp.append(buf.data(), n);
      }
      return resp;
   }

   // Read exactly one response, framed by its Content-Length (keep-alive connections).
   std::string read_one_response(asio::ip::tcp::socket& socket)
   {
      std::string resp;
      std::array<char, 4096> buf{};
      asio::error_code ec;
      for (;;) {
         const auto head_end = resp.find("\r\n\r\n");
         if (head_end != std::string::npos) {
            const auto cl_pos = resp.find("Content-Length: ");
            if (cl_pos != std::string::npos && cl_pos < head_end) {
               const auto value = resp.substr(cl_pos + 16);
               const auto declared = static_cast<std::size_t>(std::strtoull(value.c_str(), nullptr, 10));
               if (resp.size() >= head_end + 4 + declared) break;
            }
            else {
               break;
            }
         }
         const std::size_t n = socket.read_some(asio::buffer(buf), ec);
         if (n == 0 || ec) break;
         resp.append(buf.data(), n);
      }
      return resp;
   }

   asio::ip::tcp::socket connect_to(asio::io_context& io_ctx, uint16_t port)
   {
      asio::ip::tcp::socket socket(io_ctx);
      asio::ip::tcp::endpoint endpoint(asio::ip::make_address(test_host), port);
      asio::error_code ec;
      socket.connect(endpoint, ec);
      return socket;
   }

   // One-shot upload on a fresh connection, closed by the server after the response.
   std::string upload(uint16_t port, std::string_view route, const std::string& body)
   {
      asio::io_context io_ctx;
      auto socket = connect_to(io_ctx, port);
      if (!socket.is_open()) return "";

      const auto head = build_head("POST", route, port, body.size(), false);
      asio::error_code ec;
      asio::write(socket, asio::buffer(head), ec);
      asio::write(socket, asio::buffer(body), ec);

      return read_until_close(socket);
   }

   std::string run_with_timeout(std::function<std::string()> action)
   {
      auto future = std::async(std::launch::async, std::move(action));
      if (future.wait_until(std::chrono::system_clock::now() + std::chrono::seconds(30)) ==
          std::future_status::ready) {
         return future.get();
      }
      return "";
   }

   std::string make_payload(std::size_t size)
   {
      std::string payload;
      payload.resize(size);
      uint32_t state = 0x12345678u;
      for (auto& byte : payload) {
         state = state * 1664525u + 1013904223u;
         byte = static_cast<char>(state >> 24);
      }
      return payload;
   }

   // Records every chunk it is handed so the test can compare against the sent bytes.
   struct recording_sink final : glz::body_sink
   {
      recording_sink(std::mutex& mutex, std::string& out, std::atomic<std::size_t>& chunks)
         : mutex_(mutex), out_(out), chunks_(chunks)
      {}

      bool write(std::string_view chunk, glz::response&) override
      {
         std::lock_guard<std::mutex> lock(mutex_);
         out_.append(chunk);
         ++chunks_;
         return true;
      }

      void finish(glz::response& res) override
      {
         std::lock_guard<std::mutex> lock(mutex_);
         res.status(200).content_type("text/plain").body("received:" + std::to_string(out_.size()));
      }

      void abort() override {}

     private:
      std::mutex& mutex_;
      std::string& out_;
      std::atomic<std::size_t>& chunks_;
   };

} // namespace

static void error_handler(std::error_code ec, std::source_location loc)
{
   std::fprintf(stderr, "Server error at %s:%d: %s\n", loc.file_name(), loc.line(), ec.message().c_str());
}

suite http_body_stream_suite = [] {
   auto io_ctx = std::make_shared<asio::io_context>();
   glz::http_server<false> server(io_ctx, error_handler);

   std::mutex mutex;
   std::string received;
   std::atomic<std::size_t> chunks{0};
   std::atomic<std::size_t> factory_calls{0};

   server.body_post("/upload", [&](const glz::request&, glz::response&) -> std::unique_ptr<glz::body_sink> {
      ++factory_calls;
      {
         std::lock_guard<std::mutex> lock(mutex);
         received.clear();
      }
      chunks = 0;
      return std::make_unique<recording_sink>(mutex, received, chunks);
   });

   // A factory that declines the request: it fills the response and returns nullptr, so
   // the server answers without reading the body.
   server.body_post("/denied", [&](const glz::request&, glz::response& res) -> std::unique_ptr<glz::body_sink> {
      res.status(403).content_type("text/plain").body("denied");
      return nullptr;
   });

   // A normal buffered route, used to prove keep-alive survives a streamed upload.
   server.post("/echo", [](const glz::request& req, glz::response& res) {
      res.status(200).content_type("text/plain").body("echo:" + std::to_string(req.body.size()));
   });

   server.bind(test_host, 0);
   const uint16_t port = server.port();
   server.start(0);
   std::thread server_thr([&] { io_ctx->run(); });

   // Second server, capped, so the 413 path can be exercised without affecting the others.
   auto capped_ctx = std::make_shared<asio::io_context>();
   glz::http_server<false> capped(capped_ctx, error_handler);
   std::atomic<std::size_t> capped_factory_calls{0};
   std::mutex capped_mutex;
   std::string capped_received;
   std::atomic<std::size_t> capped_chunks{0};

   capped.max_streamed_body_size(1024);
   capped.body_post("/upload", [&](const glz::request&, glz::response&) -> std::unique_ptr<glz::body_sink> {
      ++capped_factory_calls;
      return std::make_unique<recording_sink>(capped_mutex, capped_received, capped_chunks);
   });
   capped.bind(test_host, 0);
   const uint16_t capped_port = capped.port();
   capped.start(0);
   std::thread capped_thr([&] { capped_ctx->run(); });

   "a large body arrives in chunks and is delivered whole"_test = [&] {
      const auto payload = make_payload(40 * 1024 * 1024);
      const auto response = run_with_timeout([&] { return upload(port, "/upload", payload); });

      expect(response.find("200 OK") != std::string::npos) << "expected 200 OK, got: " << response.substr(0, 64);
      expect(body_of(response) == "received:" + std::to_string(payload.size()))
         << "finish() response body not delivered: " << body_of(response);
      expect(chunks.load() > 1u) << "expected multiple chunks, got " << chunks.load();
      {
         std::lock_guard<std::mutex> lock(mutex);
         expect(received.size() == payload.size()) << "streamed size mismatch: " << received.size();
         expect(received == payload) << "streamed bytes differ from sent bytes";
      }
   };

   "max_streamed_body_size rejects with 413 before the sink is created"_test = [&] {
      const auto payload = make_payload(4096);
      const auto response = run_with_timeout([&] { return upload(capped_port, "/upload", payload); });

      expect(response.find("413 Payload Too Large") != std::string::npos)
         << "expected 413, got: " << response.substr(0, 64);
      expect(capped_factory_calls.load() == 0u) << "factory must not run for an oversized body";
   };

   // A client that declares a huge body waits for the response before sending it, so the
   // rejection must not depend on any body byte arriving.
   "413 is answered before the client sends the declared body"_test = [&] {
      const auto response = run_with_timeout([&] {
         asio::io_context client_ctx;
         auto socket = connect_to(client_ctx, capped_port);
         if (!socket.is_open()) return std::string{};

         asio::error_code ec;
         const auto head = build_head("POST", "/upload", capped_port, 3'000'000'000ULL, false);
         asio::write(socket, asio::buffer(head), ec);

         return read_until_close(socket);
      });

      expect(response.find("413") != std::string::npos) << "expected 413, got: " << response.substr(0, 64);
   };

   "a declining factory answers without reading the body"_test = [&] {
      const auto payload = make_payload(256 * 1024);
      const auto response = run_with_timeout([&] { return upload(port, "/denied", payload); });

      expect(response.find("403") != std::string::npos) << "expected 403, got: " << response.substr(0, 64);
      expect(body_of(response) == "denied") << "expected the rejection body, got: " << body_of(response);
   };

   "keep-alive survives a streamed upload"_test = [&] {
      const auto payload = make_payload(256 * 1024);

      const auto responses = run_with_timeout([&] {
         asio::io_context client_ctx;
         auto socket = connect_to(client_ctx, port);
         if (!socket.is_open()) return std::string{};

         asio::error_code ec;
         const auto upload_head = build_head("POST", "/upload", port, payload.size(), true);
         asio::write(socket, asio::buffer(upload_head), ec);
         asio::write(socket, asio::buffer(payload), ec);
         auto first = read_one_response(socket);

         const std::string second_body = "second";
         const auto echo_head = build_head("POST", "/echo", port, second_body.size(), false);
         asio::write(socket, asio::buffer(echo_head), ec);
         asio::write(socket, asio::buffer(second_body), ec);
         auto second = read_until_close(socket);

         return first + "\n<<>>\n" + second;
      });

      const auto separator = responses.find("\n<<>>\n");
      expect(separator != std::string::npos) << "second request never answered: " << responses;
      if (separator == std::string::npos) return;

      const auto first = responses.substr(0, separator);
      const auto second = responses.substr(separator + 6);

      expect(first.find("200 OK") != std::string::npos) << "upload response: " << first.substr(0, 64);
      expect(body_of(first) == "received:" + std::to_string(payload.size())) << "upload body: " << body_of(first);
      expect(second.find("200 OK") != std::string::npos) << "echo response: " << second.substr(0, 64);
      expect(body_of(second) == "echo:6") << "echo body: " << body_of(second);
   };

   server.stop();
   io_ctx->stop();
   if (server_thr.joinable()) server_thr.join();

   capped.stop();
   capped_ctx->stop();
   if (capped_thr.joinable()) capped_thr.join();
};

int main() { return 0; }
