#include <sio/io_uring/file_handle.hpp>
#include <sio/read_batched.hpp>

#include <exec/linux/io_uring_context.hpp>
#include <exec/static_thread_pool.hpp>
#include <exec/when_any.hpp>

#include <thread>
#include <vector>
#include <deque>
#include <map>
#include <atomic>
#include <iostream>
#include <memory>

namespace sio::io_uring {

class mt_io_uring_context {
  std::deque<exec::io_uring_context> contexts_;
  std::vector<std::thread> io_workers_;
  exec::static_thread_pool pool_;
  std::map<std::pair<int, int>, exec::safe_file_descriptor> files_;
  std::map<std::pair<int, int>, seekable_byte_stream> streams_;
  int file_count;
 public:

  mt_io_uring_context(unsigned nthreads, unsigned iodepth) : pool_{2 * nthreads}, file_count{0} {
    for (int i = 0; i < nthreads; i++) {
      contexts_.emplace_back(iodepth);
      io_workers_.emplace_back([&ctx=contexts_.back()]() {
        ctx.run_until_stopped();
      });
    }
  }
  ~mt_io_uring_context() {
    for (auto &ctx: contexts_)
      ctx.request_stop();
    for (auto &worker: io_workers_)
      worker.join();
  }

  auto open_file(std::string path, uint32_t flags) {
    const auto file_id = file_count++;
    for (int i = 0; i < contexts_.size(); i++) {
      const auto key = std::make_pair(file_id, i);
      files_.emplace(key, ::open(path.c_str(), flags));
      const auto &fd = files_[key];
      streams_[key] = seekable_byte_stream{native_fd_handle{contexts_[i], fd.native_handle()}};
    }
    return std::make_pair(file_id, files_[{file_id, 0}].native_handle());
  }

  auto read_batched(int file_id, sio::async::buffers_type_of_t<seekable_byte_stream> buffers, std::span<sio::async::offset_type_of_t<seekable_byte_stream>> offsets, const std::size_t grain_size = 128) {
    auto index = std::make_shared<std::atomic<std::size_t>>(0);
    return stdexec::schedule(pool_.get_scheduler()) | stdexec::bulk(pool_.available_parallelism(), [=](std::size_t i) {
      auto stream = streams_.at({file_id, static_cast<int>(i % io_workers_.size())});
      for (;;) {
        const auto start = index->fetch_add(grain_size, std::memory_order_relaxed);
        if (start >= buffers.size()) break;
        const auto count = std::min(grain_size, buffers.size() - start);
        auto sndr = sio::async::read_batched(stream, buffers.subspan(start, count), offsets.subspan(start, count));
        stdexec::sync_wait(sndr);
      }
    });
  }
};

}