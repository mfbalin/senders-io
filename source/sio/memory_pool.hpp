/*
 * Copyright (c) 2023 Maikel Nadolski
 *
 * Licensed under the Apache License Version 2.0 with LLVM Exceptions
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *   https://llvm.org/LICENSE.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <array>
#include <atomic>
#include <cstring>
#include <mutex>
#include <span>
#include <system_error>
#include <variant>

#include "./assert.hpp"
#include "./async_allocator.hpp"
#include "./concepts.hpp"
#include "./intrusive_list.hpp"

#include <stdexec/execution.hpp>
#include <exec/finally.hpp>

namespace sio {
  class memory_resource {
    static constexpr size_t _S_max_align = alignof(max_align_t);

   public:
    memory_resource() = default;
    memory_resource(const memory_resource&) = default;
    virtual ~memory_resource() = default;

    memory_resource& operator=(const memory_resource&) = default;

    [[nodiscard]] void* allocate(size_t __bytes, size_t __alignment = _S_max_align) noexcept {
      void* ptr = do_allocate(__bytes, __alignment);
      if (ptr) {
        return ::operator new(__bytes, do_allocate(__bytes, __alignment));
      }
      return nullptr;
    }

    void deallocate(void* __p, size_t __bytes, size_t __alignment = _S_max_align) noexcept {
      return do_deallocate(__p, __bytes, __alignment);
    }

    [[nodiscard]] bool is_equal(const memory_resource& __other) const noexcept {
      return do_is_equal(__other);
    }

   private:
    virtual void* do_allocate(size_t __bytes, size_t __alignment) noexcept = 0;

    virtual void do_deallocate(void* __p, size_t __bytes, size_t __alignment) noexcept = 0;

    virtual bool do_is_equal(const memory_resource& __other) const noexcept = 0;
  };

  inline memory_resource* get_default_resource() noexcept {
    struct type : memory_resource {
      type() = default;

      void* do_allocate(size_t __bytes, size_t __alignment) noexcept override {
        return ::operator new(__bytes, std::align_val_t(__alignment));
      }

      void do_deallocate(void* __p, size_t, size_t) noexcept override {
        ::operator delete(__p);
      }

      bool do_is_equal(const memory_resource& __other) const noexcept override {
        return &__other == this;
      }
    };

    static type res{};
    return &res;
  }

  class memory_pool;

  struct memory_block {
    void* next;
    std::size_t index;
  };

  struct allocate_operation_base {
    void (*complete_)(allocate_operation_base*) noexcept {};
    memory_pool* pool_{};
    std::size_t index_{};
    allocate_operation_base* next_{};
    allocate_operation_base* prev_{};
    std::variant<void*, std::exception_ptr> result_{};
  };

  template <class Receiver>
  struct allocate_operation : allocate_operation_base {
    [[no_unique_address]] Receiver receiver_{};

    struct on_receiver_stop {
      allocate_operation* op_{};
      void operator()() const noexcept;
    };

    allocate_operation(Receiver receiver, memory_pool* pool, std::size_t index) noexcept(nothrow_move_constructible<Receiver>)
      : allocate_operation_base{[](allocate_operation_base* self) noexcept {
        auto* op = static_cast<allocate_operation*>(self);
        op->stop_callback_.reset();
        if (op->result_.index() == 0) {
          if (std::get<0>(op->result_)) {
            stdexec::set_value(static_cast<Receiver&&>(op->receiver_), std::get<0>(op->result_));
          } else {
            stdexec::set_stopped(static_cast<Receiver&&>(op->receiver_));
          }
        } else {
          stdexec::set_error(static_cast<Receiver&&>(op->receiver_), std::get<1>(op->result_));
        }
      }, pool, index}
      , receiver_(static_cast<Receiver&&>(receiver)) {
    }

    void start(stdexec::start_t) noexcept;

    using stop_token_t = stdexec::stop_token_of_t<stdexec::env_of_t<Receiver>>;
    std::optional<typename stop_token_t::template callback_type<on_receiver_stop>> stop_callback_{};
  };

  struct allocate_sender {
    memory_pool* pool_{};
    std::size_t index_{};

    using completion_signatures = stdexec::completion_signatures<
      stdexec::set_value_t(void*),
      stdexec::set_error_t(std::exception_ptr),
      stdexec::set_stopped_t()>;

    template <stdexec::receiver_of<completion_signatures> Receiver>
    allocate_operation<Receiver> connect(stdexec::connect_t, Receiver receiver) const
      noexcept(nothrow_move_constructible<Receiver>) {
      return {static_cast<Receiver&&>(receiver), pool_, index_};
    }
  };

  template <class Receiver>
  struct deallocate_operation {
    [[no_unique_address]] Receiver receiver_{};
    memory_pool* pool_{};
    void* pointer_{};
    void (*destroy_)(void*) = nullptr;

    deallocate_operation(
      Receiver receiver,
      memory_pool* pool,
      void* pointer,
      void (*destroy)(void*)) noexcept(nothrow_move_constructible<Receiver>)
      : receiver_(static_cast<Receiver&&>(receiver))
      , pool_(pool)
      , pointer_(pointer)
      , destroy_(destroy) {
    }

    void start(stdexec::start_t) noexcept;
  };

  struct deallocate_sender {
    memory_pool* pool_{};
    void* pointer_{};
    void (*destroy_)(void*) = nullptr;

    using completion_signatures = stdexec::completion_signatures<stdexec::set_value_t()>;

    template <stdexec::receiver_of<completion_signatures> Receiver>
    deallocate_operation<Receiver> connect(stdexec::connect_t, Receiver receiver) const
      noexcept(nothrow_move_constructible<Receiver>) {
      return {static_cast<Receiver&&>(receiver), pool_, pointer_, destroy_};
    }

    friend void tag_invoke(stdexec::sync_wait_t, deallocate_sender self) noexcept {
      struct rcvr {
        using is_receiver = void;

        stdexec::empty_env get_env(stdexec::get_env_t) const noexcept {
          return {};
        }

        void set_value(stdexec::set_value_t) const noexcept {
        }
      };

      auto op = stdexec::connect(self, rcvr{});
      stdexec::start(op);
    }
  };

  class memory_pool {
   private:
    template <class Receiver>
    friend struct allocate_operation;
    template <class Receiver>
    friend struct deallocate_operation;

    memory_resource* upstream_{};
    std::mutex mutex_{};
    std::array<void*, 32> block_lists_{};
    std::array<intrusive_list<&allocate_operation_base::next_, &allocate_operation_base::prev_>, 32>
      pending_allocation_{};

    void reclaim_memory(void* ptr) noexcept;

   public:
    explicit memory_pool(memory_resource* res = get_default_resource()) noexcept;
    memory_pool(const memory_pool&) = delete;
    memory_pool(memory_pool&&) = delete;
    memory_pool& operator=(const memory_pool&) = delete;
    memory_pool& operator=(memory_pool&&) = delete;
    ~memory_pool();

    allocate_sender allocate(std::size_t size, std::size_t alignment);
    deallocate_sender deallocate(void* ptr, void (*destroy)(void*) = nullptr) noexcept;
  };

  template <class Receiver>
  void allocate_operation<Receiver>::start(stdexec::start_t) noexcept {
    SIO_ASSERT(index_ >= 0 && index_ < 32);
    std::unique_lock lock(pool_->mutex_);
    void* block_ptr = pool_->block_lists_[index_];
    void* buffer = block_ptr;
    if (!buffer) {
      buffer = pool_->upstream_->allocate(1 << (index_ + 1));
      if (!buffer) {
        pool_->pending_allocation_[index_].push_back(this);
        stop_callback_.emplace(
          stdexec::get_stop_token(stdexec::get_env(receiver_)), on_receiver_stop{this});
        return;
      }
    }
    if (block_ptr) {
      void* next = nullptr;
      std::memcpy(&next, buffer, sizeof(void*));
      pool_->block_lists_[index_] = next;
    } else {
      memory_block block{nullptr, index_};
      std::memcpy(buffer, &block, sizeof(memory_block));
    }
    lock.unlock();
    void* result = static_cast<char*>(buffer) + sizeof(memory_block);
    stdexec::set_value(static_cast<Receiver&&>(receiver_), result);
  }

  template <class Receiver>
  void deallocate_operation<Receiver>::start(stdexec::start_t) noexcept {
    if (destroy_) {
      destroy_(pointer_);
    }
    auto receiver = static_cast<Receiver&&>(receiver_);
    pool_->reclaim_memory(pointer_);
    stdexec::set_value(static_cast<Receiver&&>(receiver));
  }

  template <class T>
  struct memory_pool_allocator {
    using value_type = T;
    using pointer = T*;

    memory_pool* pool_;

    constexpr memory_pool_allocator() noexcept = default;

    explicit memory_pool_allocator(memory_pool* pool) noexcept
      : pool_(pool) {
    }

    template <class S>
    constexpr explicit memory_pool_allocator(const memory_pool_allocator<S>& other) noexcept
      : pool_(other.pool_) {
    }

    template <class... Args>
    auto async_new(async::async_new_t, Args&&... args) const {
      return stdexec::let_value(
        pool_->allocate(sizeof(T), alignof(T)),
        [... args = static_cast<Args&&>(args), this](void* ptr) mutable {
          return stdexec::let_error(
            stdexec::then(
              stdexec::just(),
              [... args = std::move(args), ptr]() mutable {
                return new (ptr) T(std::move(args)...);
              }),
            [ptr, this](std::exception_ptr e) {
              return stdexec::let_value(pool_->deallocate(ptr), [e = std::move(e)] {
                return stdexec::just_error(e);
              });
            });
        });
    }

    auto async_new_array(async::async_new_array_t, std::size_t size) const {
      return stdexec::then(
        pool_->allocate(sizeof(T) * size, alignof(T)),
        [size](void* ptr) noexcept { return new (ptr) T[size]; });
    }

    deallocate_sender async_delete(async::async_delete_t, T* ptr) const noexcept;
  };

  template <class Receiver>
  void allocate_operation<Receiver>::on_receiver_stop::operator()() const noexcept {
    {
      std::scoped_lock lock{op_->pool_->mutex_};
      op_->pool_->pending_allocation_[op_->index_].erase(op_);
    }
    op_->stop_callback_.reset();
    stdexec::set_stopped(static_cast<Receiver&&>(op_->receiver_));
  }

  template <class T>
  deallocate_sender
    memory_pool_allocator<T>::async_delete(async::async_delete_t, T* ptr) const noexcept {
    return pool_->deallocate(ptr, [](void* vptr) { static_cast<T*>(vptr)->~T(); });
  }

} // namespace sio