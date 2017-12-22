$: << '.'
require 'test_config'

module OssEmulator

  class TestSuite_Bucket < TestDecorator

    def setup_suite()
      Log.info("TestSuite setup_suite : #{self.name}", "magenta_bg")
      $bucket_name_list = []
      TestUtil.clean_temp_test_files()
      TestUtil.clean_all_buckets()
    end

    def teardown_suite()
      Log.info("TestSuite teardown_suite : #{self.name}", "magenta_bg")
      TestUtil.clean_temp_test_files()
      TestUtil.clean_all_buckets()
    end

  end

  class TestCase_CreateBucket < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
    end

    def test_create_bucket()
      # InvalidBucketName
      invalid_name_list = ["1234567890abcdefghijklmnopqrstuvwxyz-", "-asakdfiweurpq1235239408", "3425233423-", 
                          "1", "12", "z", "ab", "xy-", "-mn", " - ", "hjlasdfk 123412414", "eiwrpqwe_1293480248_", 
                          "1234567890abcdefghijklmnopqrstuvwxyz-1234567890abcdefghijklmnopqrstuvwxyz-1234567890abcdefghijklmnopqrstuvwxyz-", 
                          "1234567890123456789012345678901234567890123456789012345678901234", 
                          "abcdefghijklmnopqrstuvwxyzAabcdefghijklmnopqrstuvwxyz", 
                          "abcdefghijklmnopqrstuvwxyz%1234567890", 
                          "A", "AB", "ABC", "@!$##$$$$", "*&^%%$(){}[]<>?,./.,", "_+-=!@#$%^&*", ""
                          ]
      invalid_name_list.each do |name|
        Log.info("invalid_bucket_name : #{name}")
        assert(TestUtil.create_bucket_and_validate_fail(name, 'InvalidBucketName'))
      end

      # ValidBucketName
      $bucket_name_list = ["bucket-for-delete-1", "bucket-for-delete-2", "1234567890-abcdefghijklmnopqrstuvwxyz", 
                           "123", "0-9", "abc", "p-q", 
                           "123456789012345678901234567890123456789012345678901234567890123", 
                           "abcdefghijklmnopqrstuvwxyz-01234567890",  
                          ]

      if $bucket_name_list.length>Bucket::MAX_BUCKET_NUM
        cut_length = Bucket::MAX_BUCKET_NUM - 1
        $bucket_name_list = $bucket_name_list[0..cut_length]
      end

      # append bucket list util it reaches the BUCKET_MAX_TOTAL=30
      while $bucket_name_list.length<Bucket::MAX_BUCKET_NUM do
        increase_number = $bucket_name_list.length+1
        bucket_name_increase = "abcdefghijklmnopqrstuvwxyz-01234567890---#{increase_number}"
        $bucket_name_list << bucket_name_increase
      end

      $bucket_name_list.each do |name|
        Log.info("bucket_name_list : #{name}", 'blue')
      end

      $bucket_name_list.each do |name|
        assert(TestUtil.create_bucket_and_validate_ok(name))
      end

      Log.info("total_bucket_number is #{$bucket_name_list.length}", 'green')
      assert($bucket_name_list.length==Bucket::MAX_BUCKET_NUM)

      # TooManyBuckets
      too_many_name_list = ["too-many-buckets-12347890-abcdhijpqz", "too-many-buckets-abdijxyz-234890", "too-many-buckets-uvw"]
      too_many_name_list.each do |name|
        assert(TestUtil.create_bucket_and_validate_fail(name, 'TooManyBuckets'))
      end

      # InvalidBucketName after TooManyBuckets
      list_invalid_bucketname_after_toomany_bucket = ["invalid_bucketname_after_too_many_buckets", "invalid-bucketnameQWEWIUTISNSNSDFPIWRP"]
      list_invalid_bucketname_after_toomany_bucket.each do |name|
        assert(TestUtil.create_bucket_and_validate_fail(name, 'InvalidBucketName'))
      end

    end #function

  end #class

  class TestCase_ListBucket < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
    end

    def test_list_bucket()
      bucket_names_array = []
      TestConfig.client.list_buckets.each do |bucket|
        Log.info(bucket.name, 'blue')
        bucket_names_array << bucket.name
      end
      
      Log.info("$bucket_name_list  length is #{$bucket_name_list.length}", 'green')
      Log.info("bucket_names_array length is #{bucket_names_array.length}", 'green')

      assert_equal bucket_names_array.length, Bucket::MAX_BUCKET_NUM
      assert_equal bucket_names_array.length, $bucket_name_list.length

      bucket_names_array.each do |bucket|
        if !$bucket_name_list.include?(bucket)
          assert(false, "list bucket error : the bucket #{bucket} is not in the original bucket list.")
        end
      end
    end #function

  end #class

  class TestCase_GetBucket < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
    end

    def test_get_bucket()
      $bucket_name_list[0..3].each do |bucket_name|
        Log.info(bucket_name, 'blue')
        assert(TestConfig.client.bucket_exists?(bucket_name) == true)
      end

      $bucket_name_list.each do |bucket_name|
        bucket_put_acl = TestConfig.client.get_bucket(bucket_name)
        bucket_put_acl.acl = "private"
        bucket_get_acl = TestConfig.client.get_bucket(bucket_name)
        assert(bucket_get_acl.acl=="private")
      end
    end #function

  end #class

  class TestCase_DeleteBucket < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
    end

    def test_delete_bucket()
      # NoSuchBucket
      list_no_such_bucket = ["nosuchbucket-0987654321-abcdefghijklmn", "nosuchbucket-invalid_ABCDEFG"]
      list_no_such_bucket.each do |bucket_name|
        assert(!$bucket_name_list.include?(bucket_name))
        begin
          output = TestConfig.client.delete_bucket(bucket_name)
          Log.info(output)
        rescue
          latest_error = $!.to_s
          Log.info(latest_error, 'yellow')
          assert(latest_error.include?("NoSuchBucket"))
        end
      end

      bucket_names_array = []
      TestConfig.client.list_buckets.each do |bucket|
        Log.info(bucket.name, 'blue')
        bucket_names_array << bucket.name
      end

      # BucketNotEmpty and DeleteNormallyOK
      bucket_delete_list = bucket_names_array[0..1]
      bucket_delete_list.each do |bucket_name|
        bucket = TestConfig.client.get_bucket(bucket_name)

        object_name = "object-for-bucket-delete"
        upload_file = TestResource::TEST_DATA_BASIC
        bucket.put_object(object_name, :file => upload_file)

        temp_download_file = "#{TestResource::TEST_RESOURCE_DIR}/temp_download_get_data"
        FileUtils.rm_f(temp_download_file)
        bucket.get_object(object_name, :file => temp_download_file)
        assert_equal File.size(upload_file), File.size(temp_download_file)
        FileUtils.rm_f(temp_download_file)

        assert(TestUtil.delete_bucket_and_validate_fail(bucket_name, "BucketNotEmpty"))

        output = bucket.delete_object(object_name)
        Log.info(output)

        assert(TestUtil.delete_bucket_and_validate_ok(bucket_name))
      end
    end #function

  end #class

  class TestManagerBucket

    def self.run()
      TestConfig.init()

      test_suite_bucket = TestSuite_Bucket.new(TestCase_CreateBucket)
      test_suite_bucket << TestCase_ListBucket.suite
      test_suite_bucket << TestCase_GetBucket.suite
      test_suite_bucket << TestCase_DeleteBucket.suite

      suite_list = []
      suite_list << test_suite_bucket

      suite_list.each do |suite|
        Test::Unit::UI::Console::TestRunner.run(suite)
        sleep(TestResource::TEST_INTERVAL)
      end
    rescue
      Log.last_error()
    end

  end #class

  TestManagerBucket.run()

end #module