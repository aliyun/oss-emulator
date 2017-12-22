$: << '.'
require 'test_config'
require 'testsuite_bucket'
require 'testsuite_object'

module OssEmulator

  class TestManagerAll

    def self.run()

      test_suite_bucket = TestSuite_Bucket.new(TestCase_CreateBucket)
      test_suite_bucket << TestCase_ListBucket.suite
      test_suite_bucket << TestCase_GetBucket.suite
      test_suite_bucket << TestCase_DeleteBucket.suite

      test_suite_object = TestSuite_Object.new(TestCase_PutObject)
      test_suite_object << TestCase_PutObject_Stream.suite  
      test_suite_object << TestCase_AppendObject.suite
      test_suite_object << TestCase_CopyObject.suite
      test_suite_object << TestCase_ListObject.suite
      test_suite_object << TestCase_GetObject_Meta_ACL_Delete.suite
      test_suite_object << TestCase_Bucket_Object_Full_Mix.suite
      test_suite_object << TestCase_Multipart_Resumable_Upload.suite

      test_suite_object << TestCase_PutObject_Huge.suite if TestConfig.run_full_test

      suite_list = []
      suite_list << test_suite_bucket
      suite_list << test_suite_object

      suite_list.each do |suite|
        Test::Unit::UI::Console::TestRunner.run(suite)
        sleep(TestResource::TEST_INTERVAL)
      end
    rescue
      Log.last_error()
    end

  end
  
  TestConfig.init()
  TestManagerAll.run() while TestConfig.run_test_forever

end #module
