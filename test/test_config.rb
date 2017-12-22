$: << '../lib'
$: << './lib'
$: << '..'

require 'rubygems'
require 'fileutils'
require 'simplecov'
require 'aliyun/oss'
require 'emulator/server'
require 'bundler/setup'
require 'mocha/test_unit'
require 'test/test_util'
require 'test/unit'
require 'test/unit/assertions'
require 'test/unit/ui/console/testrunner'

module OssEmulator

  module TestResource
    MAX_OBJECT_NUMBER_PER_BUCKET = 10

    TEST_RESOURCE_DIR = "../test/resources"
    TEST_DATA_BASIC   = "#{TEST_RESOURCE_DIR}/data"
    TEST_DATA_ONE     = "#{TEST_RESOURCE_DIR}/data1"
    TEST_DATA_TWO     = "#{TEST_RESOURCE_DIR}/data2"
    TEST_DATA_THREE   = "#{TEST_RESOURCE_DIR}/data3"
    TEST_DATA_APPEND  = "#{TEST_RESOURCE_DIR}/data_append"

    TEST_DATA_LIST = [TEST_DATA_BASIC, TEST_DATA_ONE, TEST_DATA_TWO, TEST_DATA_THREE, TEST_DATA_APPEND]

    TEST_DATA_APPEND_DOWNLOAD_TEMP = "#{TEST_RESOURCE_DIR}/temp_append_download"
    TEST_DATA_MULTIPARTS_TEMP = "#{TEST_RESOURCE_DIR}/temp_multiparts"

    TEST_INTERVAL = 5 # Seconds
  end # TestResource

  class TestConfig

    @@init_flag = false
    @@quiet_mode = false
    @@run_full_test = false
    @@run_test_forever = false

    @@endpoint = "127.0.0.1"
    @@access_key_id = "Key123"
    @@access_key_secret = "Secret123"
    @@client = nil

    def self.init()
      return if @@init_flag

      Log.init(Log::LOG_TEST_FILE)
      Log.set_quiet_mode(@@quiet_mode)
      Log.set_log_level(Logger::DEBUG)

      Log.info("Start to test : endpoint=#{@@endpoint}", 'green')
      @@client = Aliyun::OSS::Client.new(endpoint: @@endpoint, access_key_id: @@access_key_id, access_key_secret: @@access_key_secret)
      Aliyun::Common::Logging.set_log_file(Log::LOG_ALIYUN_SDK_FILE)

      @@init_flag = true
      Log.info("TestConfig init successfully . ", 'green')
    end

    def self.set_endpoint(endpoint)
      @@endpoint = endpoint
    end

    def self.set_client(endpoint = nil)
      @@endpoint = endpoint unless endpoint
      @@client = Aliyun::OSS::Client.new(endpoint: @@endpoint, access_key_id: @@access_key_id, access_key_secret: @@access_key_secret)
    end
    
    def self.client()
      Log.abort("Client has not been initialized, please run TestConfig.init() firstly. ") unless @@client.is_a?(Aliyun::OSS::Client) 
      @@client
    end

    def self.run_full_test()
      @@run_full_test
    end

    def self.run_test_forever()
      @@run_test_forever
    end

  end # TestConfig

  TestConfig.init()

  # TestDecorator is for test structure
  class TestDecorator < Test::Unit::TestSuite
    def initialize(test_case_class)
      super
      self << test_case_class.suite
    end

    def run(result, &progress_block)
      setup_suite()
      begin
        super(result, &progress_block)      
      ensure
        teardown_suite()
      end
    end
  end # TestDecorator

end # module
