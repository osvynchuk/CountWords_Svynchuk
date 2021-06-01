#ifndef QUEUE_H
#define QUEUE_H

#include <deque>
#include <list>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <strstream>

namespace cpp_training {

template <typename T, size_t elem_sz = 1>
class blocking_queue {
    using internal_queue_t = std::list<T>;
    //using internal_queue_t = std::deque<T>;
public:
    explicit blocking_queue(size_t max_byte_size) : m_max_byte_size(max_byte_size)  {};
    blocking_queue(const blocking_queue&) = delete;
    blocking_queue& operator = (const blocking_queue&) = delete;

    void put (const T& val) {
        {
            std::unique_lock lock(m_lock);
            m_put_cond.wait(lock, [&val, this]() {return m_byte_size + val.size() * elem_sz <= m_max_byte_size;});
            m_queue.push_front(val);
            m_byte_size += val.size() * elem_sz;
        }
        m_get_cond.notify_one();
    }

    void put (T&& val) {
        {
            std::unique_lock lock(m_lock);
            m_put_cond.wait(lock, [&val, this]() {return m_byte_size + val.size()* elem_sz <= m_max_byte_size;});
            auto sz = val.size();
            m_queue.push_front(std::move(val));
            m_byte_size += sz * elem_sz;
        }
        m_get_cond.notify_one();
    }

    T get () {
        T ret;
        {
            std::unique_lock lock(m_lock);
            m_get_cond.wait(lock, [this](){return m_byte_size > 0 || noblock_on_get;});
            if (!m_queue.empty()) {
              ret = std::move( m_queue.back() );
              m_queue.pop_back();
              m_byte_size -= ret.size() * elem_sz;
            }
        }
        m_put_cond.notify_one();
        return ret;
    }

    bool is_empty () {
        std::lock_guard lock(m_lock);
        return m_queue.empty();
    }

    void unblock_readers () {
        noblock_on_get = true;
        m_get_cond.notify_all();
    }

    size_t size() {
        std::lock_guard lock(m_lock);
        return m_queue.size();
    }

private:
    internal_queue_t m_queue;
    std::mutex m_lock;
    std::condition_variable m_put_cond;
    std::condition_variable m_get_cond;
    size_t m_max_byte_size;
    size_t m_byte_size = 0;
    volatile bool noblock_on_get = false;
};

}

#endif // QUEUE_H
