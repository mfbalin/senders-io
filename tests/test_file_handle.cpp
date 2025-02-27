#include "sio/io_uring/file_handle.hpp"
#include "sio/sequence/ignore_all.hpp"

#include <exec/task.hpp>
#include <exec/when_any.hpp>

#include <catch2/catch.hpp>

template <class Tp>
using task = exec::basic_task<
  Tp,
  exec::__task::__default_task_context_impl<exec::__task::__scheduler_affinity::__none>>;

// exec::task<void> print_file(sio::io_uring::byte_stream file) {
// char buffer[4096];
// while (true) {
//   ssize_t bytes_read = co_await file.read(sio::async::read, buffer);
//   if (bytes_read == 0) {
//     co_return;
//   }

//   ssize_t bytes_written = co_await out.write(sio::async::write, buffer);
//   if (bytes_written != bytes_read) {
//     co_return;
//   }
// }
// }

task<void> no_op_path(sio::io_uring::path_handle handle) {
  CHECK(handle.get() > 0);
  co_return;
}

task<void> no_op_file(sio::io_uring::seekable_byte_stream input) {
  CHECK(input.get() > 0);
  std::byte buffer[8]{};
  std::size_t nbytes = co_await sio::async::read_some(input, buffer, 0);
  CHECK(nbytes == 0);
  co_return;
}

template <stdexec::sender Sender>
void sync_wait(exec::io_uring_context& context, Sender&& sender) {
  stdexec::sync_wait(exec::when_any(std::forward<Sender>(sender), context.run()));
}

TEST_CASE("file_handle - Open a path", "[file_handle]") {
  exec::io_uring_context context{};
  sio::io_uring::io_scheduler scheduler{&context};
  sync_wait(
    context, sio::async::use_resources(no_op_path, sio::async::open_path(scheduler, "/dev/null")));
}

TEST_CASE("file_handle - Open a file to /dev/null", "[file_handle]") {
  exec::io_uring_context context{};
  sio::io_uring::io_scheduler scheduler{&context};
  using sio::async::mode;
  auto file = sio::async::open_file(scheduler, "/dev/null", mode::read);
  sync_wait(context, sio::async::use_resources(no_op_file, std::move(file)));
}