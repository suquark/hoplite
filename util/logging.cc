#include "logging.h"

#ifndef _WIN32
#include <execinfo.h>
#endif

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <signal.h>
#include <sstream>
#include <stdlib.h>
#include <unistd.h>
#include <string>
#include <thread>

namespace ray {

RayLogLevel RayLog::severity_threshold_ = RayLogLevel::INFO;
std::string RayLog::app_name_ = "";

void RayLog::StartRayLog(const std::string &app_name,
                         RayLogLevel severity_threshold) {
  const char *var_value = getenv("RAY_BACKEND_LOG_LEVEL");
  if (var_value != nullptr) {
    std::string data = var_value;
    std::transform(data.begin(), data.end(), data.begin(), ::tolower);
    if (data == "debug") {
      severity_threshold = RayLogLevel::DEBUG;
    } else if (data == "info") {
      severity_threshold = RayLogLevel::INFO;
    } else if (data == "warning") {
      severity_threshold = RayLogLevel::WARNING;
    } else if (data == "error") {
      severity_threshold = RayLogLevel::ERROR;
    } else if (data == "fatal") {
      severity_threshold = RayLogLevel::FATAL;
    } else {
      RAY_LOG(WARNING) << "Unrecognized setting of RAY_BACKEND_LOG_LEVEL="
                       << var_value;
    }
    RAY_LOG(INFO)
        << "Set ray log level from environment variable RAY_BACKEND_LOG_LEVEL"
        << " to " << static_cast<int>(severity_threshold);
  }
  severity_threshold_ = severity_threshold;
  app_name_ = app_name;
}

bool RayLog::IsLevelEnabled(RayLogLevel log_level) {
  return log_level >= severity_threshold_;
}

RayLog::RayLog(const char *file_name, int line_number,
               const char *function_name, RayLogLevel severity)
    : stream_(), severity_(severity){
  auto timestamp =
      std::chrono::high_resolution_clock::now().time_since_epoch().count();
  stream_ << timestamp << " " << get_app_name() << ":" << getpid() << ":" << std::this_thread::get_id()
          << " " << file_name << ":" << line_number << " " << function_name
          << " ]: ";
}

RayLog::~RayLog() {
  stream_ << "\n";
  std::cerr << stream_.str() << std::flush;
  if (severity_ == RayLogLevel::FATAL) {
    PrintBackTrace();
    std::abort();
  }
}

void RayLog::PrintBackTrace(){
#if defined(_EXECINFO_H) || !defined(_WIN32)
  void *buffer[255];
  const int calls = backtrace(buffer, sizeof(buffer) / sizeof(void *));
  backtrace_symbols_fd(buffer, calls, 1);
#endif
}

LogFunc::LogFunc(const std::string& file_name, int line_number,
                 const std::string& function_name, const std::string& message)
  : file_name_(file_name), line_number_(line_number), 
    function_name_(function_name), message_(message) {
  std::stringstream hashstampstream;
  hashstampstream << std::chrono::high_resolution_clock::now().time_since_epoch().count();
  hashstampstream >> hashstamp_;
  if (ray::RayLog::IsLevelEnabled(ray::RayLogLevel::DEBUG))
    ::ray::RayLog(file_name_.c_str(), line_number_, function_name_.c_str(), ray::RayLogLevel::DEBUG) << "[TIMELINE] [" << hashstamp_ << "] [BEGIN] " << message_;
}

LogFunc::~LogFunc() {
  if (ray::RayLog::IsLevelEnabled(ray::RayLogLevel::DEBUG))
    ::ray::RayLog(file_name_.c_str(), line_number_, function_name_.c_str(), ray::RayLogLevel::DEBUG) << "[TIMELINE] [" << hashstamp_ << "] [END]";
}

} // namespace ray
