$: << '.'
require 'test_config'

module OssEmulator

  class TestUtil

    def self.clean_all_buckets()
      TestConfig.client.list_buckets.each do |bucket|
        TestUtil.clean_one_bucket(bucket)
      end
    end

    def self.clean_one_bucket(bucket)
      if bucket.is_a?(String)
        bucket_name = bucket
        bucket = TestConfig.client.get_bucket(bucket_name)
      elsif bucket.class==Aliyun::OSS::Bucket
        bucket_name = bucket.name
      else
        Log.raise("Invalid parameter in function TestUtil.clean_one_bucket()")
      end
      Log.info("Clean bucket : #{bucket_name}")

      bucket.list_objects.each do |obj|
        Log.info("delete object : #{obj.key}")
        bucket.delete_object(obj.key)
      end

      TestConfig.client.delete_bucket(bucket_name)
    end

    def self.clean_temp_test_files()
      # check baisc testing files
      Log.info("Check basic testing resource files in test folder. ")
      TestResource::TEST_DATA_LIST.each do |basic_file|
        if !File.exist?(basic_file)
          Log.raise("The basic testing file does not exist : #{basic_file} ")
        end
      end

      # remove temp files in test folder
      Log.info("Clean temp files in test folder. ")
      Dir[File.join(TestResource::TEST_RESOURCE_DIR, "temp*")].each do |temp_file|
        Log.info(temp_file, "blue")
        if File.basename(temp_file).index("temp")==0
          FileUtils.rm_f(temp_file)
        end
      end
    end

    def self.create_bucket_and_validate_ok(bucket_name, validate_string='')
      begin
        Log.info("create_bucket normally : #{bucket_name}")
        output = TestConfig.client.create_bucket(bucket_name)
        if output==true
          return true
        elsif output==false
          Log.info("Cannot create bucket normally : #{bucket_name}")
          return false
        elsif output.is_a?(String)
          Log.info(output)
          if output=="" || output.include?("elapsed")
            return true
          elsif validate_string!=''
            return output.include?(validate_string)
          end
        else
          Log.info(output)
          Log.info("Create bucket failed in normal case : #{bucket_name}")
          return false
        end
      rescue
        Log.info($!, "yellow")
        Log.info("Create bucket failed in normal case : #{bucket_name}")
        return false
      end
    end

    def self.create_bucket_and_validate_fail(bucket_name, validate_string='')
      begin
        Log.info("create_bucket abnormally : #{bucket_name}")
        output = TestConfig.client.create_bucket(bucket_name)
        if output==false
          return true
        elsif output==true
          Log.info(output)
          Log.info("It should not create bucket in abnormal case (#{validate_string}) : #{bucket_name}")
          return false
        elsif output.is_a?(String)
          Log.info(output)
          if validate_string!=''
            return output.include?(validate_string)
          end
        else
          Log.info(output)
          Log.info("It should not create bucket in abnormal case (#{validate_string}) : #{bucket_name}")
          return false
        end
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        if validate_string!=''
          return (latest_error.include?(validate_string) || latest_error.include?("bad URI") || latest_error.include?("BadRequest"))
        end
      end
    end
    
    def self.delete_bucket_and_validate_ok(bucket_name, validate_string='')
      begin
        Log.info("delete_bucket normally : #{bucket_name}")
        output = TestConfig.client.delete_bucket(bucket_name)
        if output==true
          return true
        elsif output==false
          Log.info("Cannot delete bucket normally : #{bucket_name}")
          return false
        elsif output.is_a?(String)
          if output=="" || output.include?("elapsed")
            return true
          elsif validate_string!=''
            return output.include?(validate_string)
          end
        else
          Log.info(output)
          Log.info("Delete bucket failed in normal case : #{bucket_name}")
          return false
        end
      rescue
        Log.info($!, "yellow")
        Log.info("Delete bucket failed in normal case : #{bucket_name}")
        return false
      end
    end #function

    def self.delete_bucket_and_validate_fail(bucket_name, validate_string='')
      begin
        Log.info("delete_bucket abnormally : #{bucket_name}")
        output = TestConfig.client.delete_bucket(bucket_name)

        if output==false
          return true
        elsif output==true
          Log.info(output)
          Log.info("It should not delete bucket in abnormal case (#{validate_string}) : #{bucket_name}")
          return false
        elsif output.is_a?(String)
          Log.info(output)
          if validate_string!=''
            return output.include?(validate_string)
          end
        else
          Log.info(output)
          Log.info("It should not delete bucket in abnormal case (#{validate_string}) : #{bucket_name}")
          return false
        end
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, 'yellow')
        if validate_string!=''
          return (latest_error.include?(validate_string) || latest_error.include?("bad URI") || latest_error.include?("BadRequest"))
        end
      end
    end #function

    def self.put_object_and_validate_fail(bucket, object_name, validate_string='')
      begin
        Log.info("put_object abnormally : #{object_name}")
        if bucket.is_a?(String)
          bucket = TestConfig.client.get_bucket(bucket)
        end
        output = bucket.put_object(object_name, :file => TestResource::TEST_DATA_BASIC)

        if output==false
          return true
        elsif output==true
          Log.info(output)
          Log.info("It should not put object in abnormal case (#{validate_string}) : #{object_name}")
          return false
        elsif output.is_a?(String)
          Log.info(output)
          if validate_string!=''
            return output.include?(validate_string)
          end
        else
          Log.info(output)
          Log.info("It should not put object in abnormal case (#{validate_string}) : #{object_name}")
          return false
        end
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        if validate_string!=''
          check_list = [validate_string, "bad URI", "BadRequest", "Too Large"]
          check_list.each do |err_keyword|
            return true if latest_error.include?(err_keyword)
          end
          return false
        end
      end
    end

    def self.put_object_and_validate_ok(bucket, object_name, validate_string='')
      begin
        Log.info("put_object normally : #{object_name}")
        if bucket.is_a?(String)
          bucket = TestConfig.client.get_bucket(bucket)
        end
        if !bucket.is_a?(Aliyun::OSS::Bucket)
          Log.raise("Invalid parameter bucket in function TestUtil.put_object_and_validate_ok")
        end
        output = bucket.put_object(object_name, :file => TestResource::TEST_DATA_BASIC)

        if output==true
          return true
        elsif output==false
          Log.info(output)
          Log.info("It should put object in normal case (#{validate_string}) : #{object_name}")
          return false
        elsif output.is_a?(String)
          Log.info(output)
          if validate_string!=''
            return output.include?(validate_string)
          end
        else
          Log.info(output)
          Log.info("It should put object in normal case (#{validate_string}) : #{object_name}")
          return false
        end
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        return false
      end
    end

  end #class

end #module