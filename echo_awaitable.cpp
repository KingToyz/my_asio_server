#include <cstdlib>
#include <deque>
#include <iostream>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <asio/awaitable.hpp>
#include <asio/detached.hpp>
#include <asio/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read_until.hpp>
#include <asio/redirect_error.hpp>
#include <asio/signal_set.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/write.hpp>
#include <asio/yield.hpp>
#include <queue>
#include <chrono>

template<class T>
class MyDeque {
    int Size;
    asio::io_context* ctx;
    asio::steady_timer ConsumeTimer_;
    asio::steady_timer ProduceTime_;
    bool IsStopped;
    std::deque<T> q;
    public:
        MyDeque(asio::io_context* ctx,int size):Size(size),ctx(ctx),ConsumeTimer_(ctx->get_executor()),ProduceTime_(ctx->get_executor()),IsStopped(false){
            ConsumeTimer_.expires_at(std::chrono::steady_clock::time_point::max());
            ProduceTime_.expires_at(std::chrono::steady_clock::time_point::max());
        }
        asio::awaitable<T> ConsumeFromQueue() {
            while(!IsStopped) {
                if(!q.empty()) {
                    auto ret = q.front();
                    q.pop_front();
                    ProduceTime_.cancel_one();
                    co_return ret;
                } else {
                    asio::error_code ec;
                    co_await ConsumeTimer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));
                    if(IsStopped) {
                        co_return T{};
                    }
                }
            }
            co_return T{};
        }
        asio::awaitable<void> AddToQueue(T&& t) {
                if(IsStopped) {
                    co_return;
                }
                if(q.size()>=Size){
                    asio::error_code ec;
                    co_await ProduceTime_.async_wait(asio::redirect_error(asio::use_awaitable, ec)); 
                    if(IsStopped) {
                        co_return;
                    }
                }
                q.push_back(std::forward<T>(t));
                ConsumeTimer_.cancel_one();
                co_return;
        }
        void close() {
            IsStopped = true;
            ConsumeTimer_.cancel();
            ProduceTime_.cancel();
        }
};

MyDeque<std::pair<std::shared_ptr<asio::ip::tcp::socket>,std::string>>* queue;

asio::awaitable<void> startImpl(std::shared_ptr<asio::ip::tcp::socket> s) {
    while (s->is_open()) {
            std::vector<char>buffer(1024);
            asio::error_code ec;
            auto n = co_await asio::async_read_until(*s, asio::dynamic_buffer(buffer),"\n",asio::redirect_error(asio::use_awaitable, ec));
            if(ec) {
                if (ec == asio::error::eof) {
                    std::cout << "Connection closed by peer" << std::endl;
                } else {
                    std::cerr << "Error: " << ec.message() << std::endl;
                } 
                continue;
            }
            if (n != 0) {
                std::string str = std::string(buffer.begin(),buffer.begin()+n);
                std::cout << "get:" << str << std::endl;
                co_await queue->AddToQueue({s,std::move(str)});
            } 
        }
}

asio::awaitable<void> start(asio::io_context& ctx) {
    asio::ip::tcp::endpoint endpoint(asio::ip::make_address("127.0.0.1"), 12345);
    asio::ip::tcp::acceptor acceptor(ctx, endpoint);

    std::vector<std::shared_ptr<asio::ip::tcp::socket>> sockets;

    while (!ctx.stopped()) {
        auto socket = std::make_shared<asio::ip::tcp::socket>(ctx);
        asio::error_code ec;
        co_await acceptor.async_accept(*socket,asio::redirect_error(asio::use_awaitable, ec));
        if(ec) {
            if (ec == asio::error::eof) {
                std::cout << "Connection closed by peer" << std::endl;
            } else {
                std::cerr << "Error: " << ec.message() << std::endl;
            } 
            continue;
        }
        std::cout << "accept:" << std::endl;
        sockets.push_back(socket);
        asio::co_spawn(ctx,startImpl(socket),asio::detached);
    }

    for (auto& s : sockets) {
        s->close();
    }
}

asio::awaitable<void> consume(asio::io_context& ctx) {
    while(!ctx.stopped()) {
        auto ret = co_await queue->ConsumeFromQueue();
        if(ret.first == nullptr) {
            break;
        }
        // std::vector<char>buffer(ret.second.begin(),ret.second.end());
        auto n = co_await asio::async_write(*ret.first,asio::buffer(ret.second.c_str(),ret.second.size()),asio::use_awaitable);
    }
}

int main() {
    asio::io_context ctx;
    queue = new MyDeque<std::pair<std::shared_ptr<asio::ip::tcp::socket>,std::string>>(&ctx,100);
    asio::signal_set signals(ctx, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto){ ctx.stop();queue->close(); });
    asio::co_spawn(ctx,start(ctx),asio::detached);
    asio::co_spawn(ctx,consume(ctx),asio::detached);
    ctx.run();
}