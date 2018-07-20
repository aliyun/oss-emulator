# -*- encoding: utf-8 -*-

require 'time'
require 'logger'
require 'fileutils'

module OssEmulator

  ##
  # Logging support
  # @example
  #   require 'logging'
  #   Log.init() 
  #   Log.set_quiet_mode(false)
  #   Log.info("something", 'green')
  #   Log.fatal("something", 'red')
  class Log

    LOG_DEFAULT_DIR = "log"
    LOG_DEFAULT_FILE = "#{LOG_DEFAULT_DIR}/emulator.log"
    LOG_TEST_FILE = "#{LOG_DEFAULT_DIR}/test.log"
    LOG_ALIYUN_SDK_FILE = "#{LOG_DEFAULT_DIR}/aliyun_sdk.log"
    LOG_FILE_SHIFT_AGE  = 1000 
    LOG_FILE_SHIFT_SIZE = 10 * 1024 * 1024
    LOG_LEVEL_DICT= { 'debug' => Logger::DEBUG, 'info' => Logger::INFO, 'warn' => Logger::WARN, 'error' => Logger::ERROR, 'fatal' => Logger::FATAL }
    LOG_LEVEL_HASH= { 0 => 'debug', 1 => 'info', 2 => 'warn', 3 => 'error', 4 => 'fatal' }

    @@logger = nil
    @@quiet_mode = true

    def self.init(log_filename = nil)
      return if @@logger.is_a?(Logger)

      log_filename = LOG_DEFAULT_FILE if log_filename.nil?
      log_dir = File.dirname(log_filename)
      FileUtils.mkdir_p(log_dir) unless File.exist?(log_dir)
      
      @@logger = Logger.new(log_filename, LOG_FILE_SHIFT_AGE, LOG_FILE_SHIFT_SIZE)
      abort "Logger init failed . " if @@logger.nil?
      @@logger.level = Logger::INFO
      self.info("Logger init successfully : #{log_filename} \n")
    end

    def self.abort(str, color = 'red')
      @@logger.fatal(str)
      println("The program will shutdown abnormally . ", color) unless @@quiet_mode
      abort str
    end

    def self.raise(str, color = 'red')
      @@logger.fatal(str)
      println("Raise Exception . ", color) unless @@quiet_mode
      raise str
    end

    def self.last_error()
      if $! 
        self.fatal($!, 'red_bg')
        $!.backtrace.each { |line| self.fatal(line, 'yellow') }
      end
    end

    def self.fatal(str, color = '')
      @@logger.fatal(str)
      println(str, color) unless @@quiet_mode
    end

    def self.error(str, color = '')
      return if @@logger.level>Logger::ERROR

      @@logger.error(str)
      println(str, color) unless @@quiet_mode
    end

    def self.warn(str, color = '')
      return if @@logger.level>Logger::WARN

      @@logger.warn(str)
      println(str, color) unless @@quiet_mode
    end

    def self.info(str, color = '')
      return if @@logger.level>Logger::INFO

      @@logger.info(str)
      println(str, color) unless @@quiet_mode
    end

    def self.debug(str, color = '')
      return if @@logger.level>Logger::DEBUG

      @@logger.debug(str)
      println(str, color) unless @@quiet_mode
    end
    
    # level = Logger::DEBUG | Logger::INFO | Logger::WARN | Logger::ERROR | Logger::FATAL
    def self.set_log_level(level)
      if level.is_a?(String)
        level = level.downcase
        @@logger.level = LOG_LEVEL_DICT[level] if LOG_LEVEL_DICT.key?(level)
        return
      end

      if level.is_a?(Numeric) && level.between?(Logger::DEBUG, Logger::FATAL)
        @@logger.level = level
        return
      end
    end

    def self.level()
      LOG_LEVEL_HASH[@@logger.level]
    end

    # mode = true | false 
    def self.set_quiet_mode(mode)
      @@quiet_mode = mode
    end

    def self.set_log_formatter(level)
      @@logger.datetime_format = "%Y-%m-%d %H:%M:%S.%L"
      @@logger.formatter = proc { |severity, datetime, progname, msg|
        "#{severity}: #{datetime}: #{msg}\n"
      }
    end

    def self.println(str, color = '')
      string_println = "#{Time.now.strftime("%Y-%m-%d %H:%M:%S.%L")} : #{str}"

      color = '' unless color.is_a?(String)
      case color.downcase
      when ''
        puts string_println
      when 'red'
        puts "\033[31m#{string_println}\033[0m\n"
      when 'green'
        puts "\033[32m#{string_println}\033[0m\n"
      when 'yellow'
        puts "\033[33m#{string_println}\033[0m\n"
      when 'blue'
        puts "\033[34m#{string_println}\033[0m\n"
      when 'magenta'
        puts "\033[35m#{string_println}\035[0m\n"
      when 'cyan'
        puts "\033[36m#{string_println}\036[0m\n"
      when 'red_bg'
        puts "\033[41m#{string_println}\033[0m\n"
      when 'green_bg'
        puts "\033[42m#{string_println}\033[0m\n"
      when 'yellow_bg'
        puts "\033[43m#{string_println}\033[0m\n"
      when 'blue_bg'
        puts "\033[44m#{string_println}\033[0m\n"
      when 'magenta_bg'
        puts "\033[45m#{string_println}\033[0m\n"
      when 'cyan_bg'
        puts "\033[46m#{string_println}\033[0m\n"
      when 'quiet', 'log_only'
      else
        puts string_println
      end
    end

  end # Log

end # OssEmulator
