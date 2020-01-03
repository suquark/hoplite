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

// This is the default implementation of ray log,
// which is independent of any libs.
class CerrLog {
public:
  CerrLog(RayLogLevel severity) : severity_(severity), has_logged_(false) {}

  virtual ~CerrLog() {
    if (has_logged_) {
      std::cerr << std::endl;
    }
    if (severity_ == RayLogLevel::FATAL) {
      PrintBackTrace();
      std::abort();
    }
  }

  std::ostream &Stream() {
    has_logged_ = true;
    return std::cerr;
  }

  template <class T> CerrLog &operator<<(const T &t) {
    // if (severity_ != RayLogLevel::DEBUG) {
    has_logged_ = true;
    std::cerr << t;
    //  }
    return *this;
  }

protected:
  const RayLogLevel severity_;
  bool has_logged_;

  void PrintBackTrace() {
#if defined(_EXECINFO_H) || !defined(_WIN32)
    void *buffer[255];
    const int calls = backtrace(buffer, sizeof(buffer) / sizeof(void *));
    backtrace_symbols_fd(buffer, calls, 1);
#endif
  }
};

typedef ray::CerrLog LoggingProvider;

RayLogLevel RayLog::severity_threshold_ = RayLogLevel::INFO;
std::string RayLog::app_name_ = "";
std::string RayLog::log_dir_ = "";

void RayLog::StartRayLog(const std::string &app_name,
                         RayLogLevel severity_threshold,
                         const std::string &log_dir) {
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
  log_dir_ = log_dir;
}

bool RayLog::IsLevelEnabled(RayLogLevel log_level) {
  return log_level >= severity_threshold_;
}

RayLog::RayLog(const char *file_name, int line_number,
               const char *function_name, RayLogLevel severity)
    // glog does not have DEBUG level, we can handle it using is_enabled_.
    : logging_provider_(nullptr), is_enabled_(severity >= severity_threshold_) {
  auto logging_provider = new CerrLog(severity);
  auto timestamp =
      std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::stringstream ss;
  ss << timestamp << " " << get_app_name() << ":" << getpid() << ":" << std::this_thread::get_id()
     << " " << file_name << ":" << line_number << " " << function_name
     << " ]: ";
  *logging_provider << ss.str();
  logging_provider_ = logging_provider;
}

std::ostream &RayLog::Stream() {
  auto logging_provider =
      reinterpret_cast<LoggingProvider *>(logging_provider_);
  return logging_provider->Stream();
}

bool RayLog::IsEnabled() const { return is_enabled_; }

RayLog::~RayLog() {
  if (logging_provider_ != nullptr) {
    delete reinterpret_cast<LoggingProvider *>(logging_provider_);
    logging_provider_ = nullptr;
  }
}

LogFunc::LogFunc(const std::string& file_name, int line_number,
                 const std::string& function_name, const std::string& message)
  : file_name_(file_name), line_number_(line_number), 
    function_name_(function_name), message_(message) {
  // std::stringstream hashstampstream;
  // hashstampstream << std::chrono::high_resolution_clock::now().time_since_epoch().count();
  // hashstamp_ = hashstampstream.str();
  // hashstamp_ = "";
  if (ray::RayLog::IsLevelEnabled(ray::RayLogLevel::INFO))
    ::ray::RayLog(file_name_.c_str(), line_number_, function_name_.c_str(), ray::RayLogLevel::INFO) << "[TIMELINE] [ ] [BEGIN] " << message_;
}

LogFunc::~LogFunc() {
  if (ray::RayLog::IsLevelEnabled(ray::RayLogLevel::INFO))
    ::ray::RayLog(file_name_.c_str(), line_number_, function_name_.c_str(), ray::RayLogLevel::INFO) << "[TIMELINE] [ ] [END] " << message_;
}

} // namespace ray
