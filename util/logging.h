#ifndef RAY_UTIL_LOGGING_H
#define RAY_UTIL_LOGGING_H

#include <iostream>
#include <sstream>
#include <string>

namespace ray {

enum class RayLogLevel {
  DEBUG = -1,
  INFO = 0,
  WARNING = 1,
  ERROR = 2,
  FATAL = 3
};

#define RAY_LOG_INTERNAL(level)                                                \
  ::ray::RayLog(__FILE__, __LINE__, __func__, level)

#define RAY_LOG_ENABLED(level)                                                 \
  ray::RayLog::IsLevelEnabled(ray::RayLogLevel::level)

#define RAY_LOG(level)                                                         \
  if (RAY_LOG_ENABLED(level))                                                  \
  RAY_LOG_INTERNAL(ray::RayLogLevel::level)

#define RAY_IGNORE_EXPR(expr) ((void)(expr))

#define RAY_CHECK(condition)                                                   \
  (condition) ? RAY_IGNORE_EXPR(0)                                             \
              : ::ray::Voidify() & ::ray::RayLog(__FILE__, __LINE__, __func__, \
                                                 ray::RayLogLevel::FATAL)      \
                                       << " Check failed: " #condition " "

#ifdef NDEBUG

#define RAY_DCHECK(condition)                                                  \
  (condition) ? RAY_IGNORE_EXPR(0)                                             \
              : ::ray::Voidify() & ::ray::RayLog(__FILE__, __LINE__, __func__, \
                                                 ray::RayLogLevel::ERROR)      \
                                       << " Debug check failed: " #condition   \
                                          " "
#else

#define RAY_DCHECK(condition) RAY_CHECK(condition)

#endif // NDEBUG

// Alias
#define LOG RAY_LOG
#define DCHECK RAY_DCHECK
#define LOGFUNC(message)                                                                \
  ::ray::LogFunc _logme(__FILE__, __LINE__, __func__, message)
// To make the logging lib plugable with other logging libs and make
// the implementation unawared by the user, RayLog is only a declaration
// which hide the implementation into logging.cc file.
// In logging.cc, we can choose different log libs using different macros.

// This is also a null log which does not output anything.

class RayLog {
public:
  RayLog(const char *file_name, int line_number, const char *function_name,
         RayLogLevel severity);

  ~RayLog();

  /// The init function of ray log for a program which should be called only
  /// once.
  ///
  /// \parem appName The app name which starts the log.
  /// \param severity_threshold Logging threshold for the program.
  /// \param logDir Logging output file name. If empty, the log won't output to
  /// file.
  static void StartRayLog(const std::string &appName,
                          RayLogLevel severity_threshold = RayLogLevel::INFO);

  /// Return whether or not the log level is enabled in current setting.
  ///
  /// \param log_level The input log level to test.
  /// \return True if input log level is not lower than the threshold.
  static bool IsLevelEnabled(RayLogLevel log_level);

  // Get the log level from environment variable.
  static RayLogLevel GetLogLevelFromEnv();

  inline static const std::string &get_app_name() { return app_name_; }

  template <typename T> RayLog &operator<<(const T &t) {
    stream_ << t;
    return *this;
  }


private:
  // Hide the implementation of log provider by void *.
  // Otherwise, lib user may define the same macro to use the correct header
  // file.
  std::stringstream stream_;
  /// True if log messages should be logged and false if they should be ignored.
  static RayLogLevel severity_threshold_;
  // In InitGoogleLogging, it simply keeps the pointer.
  // We need to make sure the app name passed to InitGoogleLogging exist.
  static std::string app_name_;
};

// This class make RAY_CHECK compilation pass to change the << operator to void.
// This class is copied from glog.
class Voidify {
public:
  Voidify() {}
  // This has to be an operator with a precedence lower than << but
  // higher than ?:
  void operator&(RayLog &) {}
};

class LogFunc {
public:
  LogFunc(const std::string& file_name, int line_number,
          const std::string& function_name, const std::string& message);
  ~LogFunc();
private:
  std::string file_name_;
  int line_number_;
  std::string function_name_;
  std::string message_;
  std::string hashstamp_;
};

} // namespace ray

#endif // RAY_UTIL_LOGGING_H
