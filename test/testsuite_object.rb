$: << '.'
require 'test_config'

module OssEmulator

  class TestSuite_Object < TestDecorator

    def setup_suite()
      Log.info("TestSuite setup_suite : #{self.name}", "magenta_bg")
      TestUtil.clean_temp_test_files()
      TestUtil.clean_all_buckets()
    end

    def teardown_suite()
      Log.info("TestSuite teardown_suite : #{self.name}", "magenta_bg")
      TestUtil.clean_temp_test_files()
      TestUtil.clean_all_buckets()
    end

  end #class

  class TestCase_PutObject < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
      assert_not_nil TestConfig.client
      @bucket_put_object = "bucket-put-object"
      assert(TestUtil.create_bucket_and_validate_ok(@bucket_put_object))
      Log.info("#{Object::MAX_OBJECT_NAME_LENGTH}", "green")
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
      TestUtil.clean_one_bucket(@bucket_put_object)
    end

    def test_put_object()
      # NoSuchBucket causes put_object failure
      bucket_name_nosuchbucket = "bucket-not-exists"
      object_name_basedon_nosuchbuckets = "object-basedon_nosuchbucket"
      assert(TestUtil.create_bucket_and_validate_ok(bucket_name_nosuchbucket))
      bucket_not_exists = TestConfig.client.get_bucket(bucket_name_nosuchbucket)
      assert(TestUtil.delete_bucket_and_validate_ok(bucket_name_nosuchbucket))
      assert(TestUtil.put_object_and_validate_fail(bucket_not_exists, object_name_basedon_nosuchbuckets, 'NoSuchBucket'))

      # InvalidObjectName
      o = [('0'..'9'),('a'..'z'),('A'..'Z')].map{|i| i.to_a}.flatten
      invalid_too_long_name = (0..(Object::MAX_OBJECT_NAME_LENGTH+1)).map{ o[rand(o.length)] }.join
      invalid_long_name = (0..(Object::MAX_OBJECT_NAME_SLICE_LENGTH+1)).map{ o[rand(o.length)] }.join

      invalid_object_name_list = [invalid_too_long_name, invalid_long_name, 
                                  "\\abcde-198932_TRMSDIUIERU", #"/08237-+_=POMHE", "/",  
                                 ]
      invalid_object_name_list.each do |invalid_obj_name|
        assert(TestUtil.put_object_and_validate_fail(@bucket_put_object, invalid_obj_name, 'InvalidObjectName'))
      end

      # ValidObjectName
      o = [('0'..'9'),('a'..'z'),('A'..'Z')].map{|i| i.to_a}.flatten
      valid_long_name_max = (1..Object::MAX_OBJECT_NAME_SLICE_LENGTH).map{ o[rand(o.length)] }.join
      valid_object_name_list = [valid_long_name_max, "object-put-1", "object-put-2", "object-put-3", 
                                "object-for-delete-1", "object-for-delete-2", "Lu_123-abc+opq", 
                                "_ABCDEFGHIJKLMNOPQRSTUVWXYZ-abcdefghijklmnopqrstuvwxyz-01234567890-",  
                                "-abcdefghijklmnopqrstuvwxyz-01234567890_ABCDEFGHIJKLMNOPQRSTUVWXYZ_", 
                                "A\\Z", "9X8", "a0b\\", "a0b\\\\", "123\\456\\789", "abc\\lmn\\rst",
                                "AAA/BBB", "ZZZ/5X8", "987/012_ABC/OPiQAZ", "xxx/a0b\\", "a0b\\\\",  
                               ]
      valid_object_name_list.each do |obj_name|
        assert(TestUtil.put_object_and_validate_ok(@bucket_put_object, obj_name))
      end

      # Long ValidObjectName : 1023 Bytes
      max_object_length = Object::MAX_OBJECT_NAME_LENGTH
      max_slice_length = Object::MAX_OBJECT_NAME_SLICE_LENGTH
      object_name_set = [('0'..'9'), ('a'..'z'), ('A'..'Z')].map{|i| i.to_a}.flatten
      object_name = ""
      while object_name.length<=max_object_length
        slice_name = (1..rand(1..max_slice_length)).map{ object_name_set[rand(object_name_set.length)] }.join
        if object_name!=""
          object_name = object_name + '/' + slice_name
        else
          object_name = slice_name
        end
      end
      object_name = object_name[0..(max_object_length-1)]
      assert(TestUtil.put_object_and_validate_ok(@bucket_put_object, object_name))

      # Long InvalidObjectName 
      object_name = object_name + '/' + (1..rand(1..max_slice_length)).map{ object_name_set[rand(object_name_set.length)] }.join
      assert(TestUtil.put_object_and_validate_fail(@bucket_put_object, object_name, 'InvalidObjectName'))

    end #function

  end #class

  class TestCase_PutObject_Stream < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
      assert_not_nil TestConfig.client
      @bucket_put_object_stream = "bucket-put-object-stream"
      assert(TestUtil.create_bucket_and_validate_ok(@bucket_put_object_stream))
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
      TestUtil.clean_one_bucket(@bucket_put_object_stream)
    end

    def test_put_object_stream()
      bucket = TestConfig.client.get_bucket(@bucket_put_object_stream)

      # Valid Large Stream
      Log.info("Test Valid Large Stream")
      object_name_large_stream = "object-large-stream-valid"
      input_size = 0
      test_input_size = 50*1024*1024 # 50MB
      Log.info("src_object_size=#{test_input_size} Bytes", "blue")
      begin
        bucket.put_object(object_name_large_stream) do |stream|
          while input_size<=test_input_size
            s = "#{(rand * 100).to_s}"
            stream << s
            input_size += s.size
          end
        end

        Log.info("object_size=#{input_size} Bytes", "blue")
        assert(input_size<=Object::MAX_OBJECT_FILE_SIZE)
        assert(true)
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        Log.info("object_size=#{input_size} Bytes", "yellow")
        assert(input_size<=Object::MAX_OBJECT_FILE_SIZE)
        assert(false)
      end

      begin
        get_output = ""
        output_size = 0
        bucket.get_object(object_name_large_stream) do |chunk|
            get_output = chunk
            output_size += chunk.size
        end
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(false)
      end

      Log.info("put_object_size=#{input_size} Bytes, get_object_size=#{output_size} Bytes")
      assert_equal input_size, output_size
    end #function

  end #class

  class TestCase_AppendObject < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
      assert_not_nil TestConfig.client
      @bucket_name_append_object = "bucket-append-object"
      assert(TestUtil.create_bucket_and_validate_ok(@bucket_name_append_object))
      @bucket = TestConfig.client.get_bucket(@bucket_name_append_object)
      assert_not_nil(@bucket)
      @object_name_append = "object-for-append"
      @object_cannot_appendable = "object-cannot-appendable"
      @temp_file_append_download = TestResource::TEST_DATA_APPEND_DOWNLOAD_TEMP
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
      TestUtil.clean_one_bucket(@bucket_name_append_object)
      FileUtils.rm_f(@temp_file_append_download)
    end

    def test_append_object_not_appendable()
      Log.debug("test_append_object_not_appendable. ")
      assert(TestUtil.put_object_and_validate_ok(@bucket, @object_cannot_appendable))
      
      Log.debug("It should be failed while Create append_object file to object which is not appendable. ")
      begin
        @bucket.append_object(@object_cannot_appendable, 0) {}
        assert(false)
      rescue
        latest_error = $!.to_s
        Log.debug(latest_error, 'yellow')
        assert(latest_error.include?("ObjectNotAppendable"))
      end

      Log.debug("It should be failed while append_object to object which is not appendable. ")
      pos = File.size(TestResource::TEST_DATA_BASIC)
      begin
        @bucket.append_object(@object_cannot_appendable, pos, :file => TestResource::TEST_DATA_ONE)
        assert(false)
      rescue
        latest_error = $!.to_s
        Log.debug(latest_error, 'yellow')
        assert(latest_error.include?("ObjectNotAppendable"))
      end
      
    rescue
      latest_error = $!.to_s
      Log.info(latest_error, "yellow")
      assert(false)
    end

    def test_append_object()
      begin
        # Create file 
        Log.debug("Create append_object file. ")
        @bucket.append_object(@object_name_append, 0) {}

        # Append file to object
        Log.debug("Append file to object. ")
        next_pos = @bucket.append_object(@object_name_append, 0) do |stream|
            100.times { |i| stream << i.to_s }
        end

        # Append file again 
        Log.debug("Append file again. ")
        next_pos = @bucket.append_object(@object_name_append, next_pos, :file => TestResource::TEST_DATA_ONE)
        next_pos = @bucket.append_object(@object_name_append, next_pos, :file => TestResource::TEST_DATA_TWO)

        Log.debug("Append file compare. ")
        file_append_for_compare = TestResource::TEST_DATA_APPEND
        FileUtils.rm_f(@temp_file_append_download)

        Log.debug("Get append file. ")
        @bucket.get_object(@object_name_append, :file => @temp_file_append_download)

        Log.debug("#{File.size(file_append_for_compare)}, #{File.size(@temp_file_append_download)}")
        assert_equal File.size(file_append_for_compare), File.size(@temp_file_append_download)
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(false)
      end
    end #function

  end #class

  class TestCase_CopyObject < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
      assert_not_nil TestConfig.client
      @bucket_src_copy_object = "bucket-src-copy-object"
      assert(TestUtil.create_bucket_and_validate_ok(@bucket_src_copy_object))
      @bucket = TestConfig.client.get_bucket(@bucket_src_copy_object)
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
      TestUtil.clean_one_bucket(@bucket_src_copy_object)
    end

    def test_copy_object_in_same_bucket()
      begin
        file_data = TestResource::TEST_DATA_BASIC
        object_src = "obj-data-src"
        @bucket.put_object(object_src, :file => file_data)
        output = @bucket.get_object_meta(object_src)
        src_last_modified = output.last_modified
        Log.info(src_last_modified) 
        sleep(2)

        # CopyMode : Aliyun::OSS::MetaDirective::COPY
        Log.info("Mode : Aliyun::OSS::MetaDirective::COPY")
        object_dst_copy = "obj-data-dst-copy"
        @bucket.copy_object(object_src, object_dst_copy, :meta_directive => Aliyun::OSS::MetaDirective::COPY)
        file_object_copy = "#{TestResource::TEST_RESOURCE_DIR}/temp-data-copy-download"

        FileUtils.rm_f(file_object_copy)
        @bucket.get_object(object_dst_copy, :file => file_object_copy)
        assert_equal File.size(file_data), File.size(file_object_copy)
        FileUtils.rm_f(file_object_copy)

        output = @bucket.get_object_meta(object_dst_copy)
        dst_copy_last_modified = output.last_modified
        Log.info(src_last_modified) 
        # metadata should be equal in COPY mode
        assert_equal(src_last_modified, dst_copy_last_modified)

        # CopyMode : Aliyun::OSS::MetaDirective::REPLACE
        Log.info("Mode : Aliyun::OSS::MetaDirective::REPLACE")
        object_dst_copy_replace = "obj-data-dst-copy-replace"
        @bucket.copy_object(object_src, object_dst_copy_replace, :meta_directive => Aliyun::OSS::MetaDirective::REPLACE)
        file_object_copy_replace = "#{TestResource::TEST_RESOURCE_DIR}/temp-data-copy-replace-download"

        FileUtils.rm_f(file_object_copy_replace)
        @bucket.get_object(object_dst_copy_replace, :file => file_object_copy_replace)
        assert_equal File.size(file_data), File.size(file_object_copy_replace)
        FileUtils.rm_f(file_object_copy_replace)

        output = @bucket.get_object_meta(object_dst_copy_replace)
        dst_copy_last_modified = output.last_modified
        Log.info(dst_copy_last_modified) 
        # metadata should not be different in REPLACE mode
        assert(src_last_modified!=dst_copy_last_modified)
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(false)
      end
    end #function

  end #class

  class TestCase_ListObject < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
      assert_not_nil TestConfig.client
      @bucket_name_list_object = "bucket-for-list-object"
      assert(TestUtil.create_bucket_and_validate_ok(@bucket_name_list_object))
      @bucket = TestConfig.client.get_bucket(@bucket_name_list_object)
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
      TestUtil.clean_one_bucket(@bucket_name_list_object)
    end

    def test_list_object_delete_multiple_objects()
      begin
        objects_list = {'data1' => TestResource::TEST_DATA_BASIC, 
                        'my-object' => TestResource::TEST_DATA_ONE, 
                        'my-obj-data3' => TestResource::TEST_DATA_TWO, 
                        'my-obj-data4' => TestResource::TEST_DATA_THREE, 
                        'my-obj-ZXYbdeafeewtOIPHKIPEWHKSCZNVZAOSWUIWRUO' => TestResource::TEST_DATA_BASIC, 
                        'abdeaYQWwtOIP-my-obj-HSIPUYTHASHFO9876541230' => TestResource::TEST_DATA_BASIC, 
                        'data5' => TestResource::TEST_DATA_BASIC,
                        'my-OBJ-ABD123542-0-08381238' => TestResource::TEST_DATA_BASIC,
                        'my-AUIYITBMNMWORQP' => TestResource::TEST_DATA_BASIC,
                        'ZZAHWDUIYQWO-D_IH-my-obj-FWZZ-AUIYITBMNQP' => TestResource::TEST_DATA_BASIC,
                        'my-obj-LKJHGBNMVFRTEEEWEWTUIIASWCZXC-adsfjquiwernviqewunvisfiqweu' => TestResource::TEST_DATA_BASIC, 
                        'my-objiwqepfnwkvqnhnqwuenevnqwvn-my-obj' => TestResource::TEST_DATA_BASIC, 
                        'my-obj' => TestResource::TEST_DATA_BASIC  
                       }
        # Put all the objects in the list
        objects_list.keys.each do |(key, value)|
            @bucket.put_object(key, :file => value)
        end

        # List All Objects
        Log.info("List all objects", "blue")
        objects = @bucket.list_objects
        objects_name_list = []
        objects.each { |o| objects_name_list << o.key }
        Log.info("#{objects_name_list}")
        assert_equal(objects_list.keys.sort, objects_name_list.sort)

        # List Objects with prefix 
        prefix_string = "my-"
        Log.info("List objects with prefix '#{prefix_string}' ", "blue")
        original_objects_list = objects_list.keys.find_all{|item| item.index(prefix_string)==0}
        original_objects_list.sort!

        objects = @bucket.list_objects(:prefix => prefix_string)
        objects_name_list = []
        objects.each { |o| objects_name_list << o.key }
        objects_name_list.sort!

        Log.info("#{objects_name_list}")
        assert_equal(original_objects_list, objects_name_list)

        # List Objects sorted after marker with prefix
        marker_string = "my-obj"
        Log.info("List objects sorted after marker '#{marker_string}' with prefix '#{prefix_string}' ", "blue")

        original_objects_list_marker = []
        marker_found = false
        original_objects_list.each do |item|
          cmp = marker_string<=>item
          if cmp<=0
            original_objects_list_marker << item if marker_found
            marker_found = true
          end
        end
        original_objects_list_marker.sort!

        objects = @bucket.list_objects(:prefix => prefix_string, :marker => marker_string)
        objects_name_list = []
        objects.each { |o| objects_name_list << o.key }
        objects_name_list.sort!
        
        Log.info("#{objects_name_list}")
        assert_equal(original_objects_list_marker, objects_name_list)

        # DeleteMultipleObjects without quite mode
        delete_quiet_list = objects_list.keys[0..2]
        result = @bucket.batch_delete_objects(delete_quiet_list, :quiet => false)
        assert_equal result, objects_list.keys[0..2]

        # DeleteMultipleObjects with quite mode
        delete_not_quite_list = objects_list.keys[3..5]
        result = @bucket.batch_delete_objects(delete_not_quite_list, :quiet => true)
        assert_equal result, []
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(false)
      end
    end #function

  end #class

  class TestCase_GetObject_Meta_ACL_Delete < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
      assert_not_nil TestConfig.client
      @bucket_name = "bucket-for-get-object-meta-acl-delete"
      assert(TestUtil.create_bucket_and_validate_ok(@bucket_name))
      @bucket = TestConfig.client.get_bucket(@bucket_name)
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
      TestUtil.clean_one_bucket(@bucket_name)
    end

    def test_get_object_meta_acl_delete_normal()
      begin
        name_header_set = [('0'..'9'), ('a'..'z'), ('A'..'Z')].map{|i| i.to_a}.flatten
        name_set = name_header_set + ['_', '-']
        object_name_list = []
        (0..30).each do 
          name_length = rand(1..Object::MAX_OBJECT_NAME_SLICE_LENGTH)
          name = (1..name_length).map{ name_set[rand(name_set.length)] }.join
          name[0] = name_header_set[rand(name_header_set.length)] if (name[0]='\\' || name[0]='/') 
          object_name_list << name
        end
        object_name_list.uniq!

        temp_file = "#{TestResource::TEST_RESOURCE_DIR}/temp_get_object"
        object_name_list.each do |obj|
          # put object
          assert(TestUtil.put_object_and_validate_ok(@bucket, obj))

          # get object and validate 
          FileUtils.rm_f(temp_file)
          @bucket.get_object(obj, :file => temp_file)
          assert_equal File.size(TestResource::TEST_DATA_BASIC), File.size(temp_file)
          FileUtils.rm_f(temp_file)

          # meta data
          obj_meta = @bucket.get_object_meta(obj)
          assert_equal obj_meta.key, obj

          # set acl and get acl
          acl_set = "default"
          @bucket.set_object_acl(obj, acl_set)
          acl_get = @bucket.get_object_acl(obj)
          assert_equal acl_get, "default"
        end

        object_name_list.each do |obj|
          @bucket.delete_object(obj)
        end
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(false)
      end
    end #function

    def test_get_object_meta_acl_delete_abnormal()
      object_name_nonexistent = "nonexistent-object-name"
      temp_file = "#{TestResource::TEST_RESOURCE_DIR}/temp_get_object_abnormal"

      # get_object : NotFound/NoSuchKey
      begin
        @bucket.get_object(object_name_nonexistent, :file => temp_file)
        assert(false)
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(latest_error.include?("NotFound"))
      end
      
      # get_object_meta : NotFound/NoSuchKey/404
      begin
        @bucket.get_object_meta(object_name_nonexistent)
        assert(false)
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(latest_error.include?("NotFound") || latest_error.include?("404"))
      end

      # set_object_acl : NotFound/NoSuchKey
      begin
        @bucket.set_object_acl(object_name_nonexistent, "default")
        assert(false)
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(latest_error.include?("NotFound"))
      end

      # get_object_acl : NotFound/NoSuchKey
      begin
        @bucket.get_object_acl(object_name_nonexistent)
        assert(false)
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(latest_error.include?("NotFound"))
      end

      FileUtils.rm_f(temp_file)
    end #function

  end #class

  class TestCase_Bucket_Object_Full_Mix < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
      assert_not_nil TestConfig.client
      TestUtil.clean_all_buckets()
      @total_bucket_number = Bucket::MAX_BUCKET_NUM
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
      TestUtil.clean_all_buckets()
    end

    def test_bucket_object_full_mix()
      begin
        # bucket name set
        bucket_name_set = "1234567890abcdefghijklmnopqrstuvwxyz-"
        bucket_name_bounder_set = "1234567890abcdefghijklmnopqrstuvwxyz"

        # object name set
        object_name_header_set = [('0'..'9'), ('a'..'z'), ('A'..'Z')].map{|i| i.to_a}.flatten
        object_name_set = object_name_header_set + ['_', '-', '\\', '/']

        bucket_hash = {}
        bucket_number = @total_bucket_number
        (1..bucket_number).each do
          # generate one bucket name
          bucket_name = (1..rand(3..Bucket::MAX_BUCKET_NAME_LENGTH)).map{ bucket_name_set[rand(bucket_name_set.length)] }.join
          bucket_name[0] = bucket_name_bounder_set[rand(bucket_name_bounder_set.length)] if bucket_name[0]=='-'
          bucket_name[-1] = bucket_name_bounder_set[rand(bucket_name_bounder_set.length)] if bucket_name[-1]=='-'
          bucket_hash[bucket_name] = []

          # generate object names under the bucket
          object_number = rand(1..TestResource::MAX_OBJECT_NUMBER_PER_BUCKET)
          (1..object_number).each do
            object_name = ""
            slice_number = rand(1..15)
            slice_number = 1 if slice_number>9

            (1..slice_number).each do
              slice_name = (1..rand(1..Object::MAX_OBJECT_NAME_SLICE_LENGTH)).map{ object_name_set[rand(object_name_set.length)] }.join
              if object_name!=""
                object_name = object_name + '/' + slice_name
              else
                object_name = slice_name
              end

              if object_name.length>=Object::MAX_OBJECT_NAME_LENGTH
                object_name = object_name[0..(Object::MAX_OBJECT_NAME_LENGTH-1)]
                break
              end
            end
            
            object_name[0] = object_name_header_set[rand(object_name_header_set.length)] if (object_name[0]=='\\' || object_name[0]=='/')
            object_name[-1] = object_name_header_set[rand(object_name_header_set.length)] if (object_name[-1]=='\\' || object_name[-1]=='/')
            if !( object_name.include?("\\\\") || object_name.include?("//") )
              bucket_hash[bucket_name] << object_name
            end
          end
        end
        assert(bucket_hash.length<=Bucket::MAX_BUCKET_NUM)

        # create buckets and objects
        bucket_hash.each do |b, v|
          # create bucket
          assert(TestUtil.create_bucket_and_validate_ok(b))

          v.each do |o|
            # put object
            assert(TestUtil.put_object_and_validate_ok(b, o))
          end
        end

        # list buckets and objects
        real_bucket_hash = {}
        TestConfig.client.list_buckets.each do |b|
          real_bucket_hash[b.name] = []

          b.list_objects.each do |obj|
            real_bucket_hash[b.name] << obj.key
          end
        end

        # compare the original bucket_hash and the real_bucket_hash
        bucket_hash.keys.each do |b|
          bucket_hash[b].sort!
          real_bucket_hash[b].sort!
          if bucket_hash[b] != real_bucket_hash[b]
            bucket_hash[b].each do |bi|
              if !real_bucket_hash[b].include?(bi)
                Log.info(bi, "red_bg")
                Log.info(bucket_hash[b], "blue")
                Log.info(real_bucket_hash[b], "yellow")
              end
            end
            assert(false)
          end
        end

      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(false)
      end
    end #function

  end #class

  class TestCase_Multipart_Resumable_Upload < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
      assert_not_nil TestConfig.client
      @bucket_name_resumable = "bucket-resumable-upload"
      assert(TestUtil.create_bucket_and_validate_ok(@bucket_name_resumable))
      @bucket = TestConfig.client.get_bucket(@bucket_name_resumable)
      assert_not_nil(@bucket)

      # create temp source multiparts file
      @file_multiparts = TestResource::TEST_DATA_MULTIPARTS_TEMP 
      FileUtils.rm_f(@file_multiparts)
      source_file_size = 50 * 1024 * 1024 # 50M 
      File.open(@file_multiparts, 'w') do |f| 
        while f.size<source_file_size
          f.puts("ABCDEFGHIJKLMNOPQRSTUVWXYZ-abcdefghijklmnopqrstuvwxyz-01234567890-")
        end
      end
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
      TestUtil.clean_one_bucket(@bucket_name_resumable)
      FileUtils.rm_f(@file_multiparts)
    end

    def test_multipart_resumable_upload()
      # multipart_resumable_upload
      begin
        object_multiparts = "object_multiparts"
        part_size = 3 * 1024 * 1024  # 3M 
        @bucket.resumable_upload(object_multiparts, @file_multiparts, 
          :part_size => part_size, :disable_cpt => true, :threads => 5) { |p|
            Log.info("Progress: #{(p*100).to_i}%")
          }
      rescue 
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(false, "multipart_resumable_upload exception : #{@bucket_name_resumable}, #{object_multiparts} ")
      end
      # multiparts_get_object
      begin
        data_multiparts_download = "#{TestResource::TEST_RESOURCE_DIR}/temp_multiparts_download"
        FileUtils.rm_f(data_multiparts_download)
        @bucket.get_object(object_multiparts, :file => data_multiparts_download)
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(false, "get_object exception : #{@bucket_put_object_stream}, #{object_name_large_stream} ")
      end
      assert_equal File.size(@file_multiparts), File.size(data_multiparts_download)
      FileUtils.rm_f(data_multiparts_download)
      # multiparts_resumable_download
      begin
        temp_file_multiparts_resumable_download = "#{TestResource::TEST_RESOURCE_DIR}/temp_multiparts_resumable_download"
        FileUtils.rm_f(temp_file_multiparts_resumable_download)
        Log.info("multiparts_resumable_download : #{temp_file_multiparts_resumable_download}", "blue")
        part_size = 3 * 1024 * 1024  
        @bucket.resumable_download(object_multiparts, temp_file_multiparts_resumable_download,
            :part_size => part_size, :disable_cpt => true, :threads => 5) { |p|
                Log.info("Progress:#{(p*100).to_i}%")
            }
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(false)
      end
      assert_equal File.size(@file_multiparts), File.size(temp_file_multiparts_resumable_download)
      FileUtils.rm_f(temp_file_multiparts_resumable_download)

    end #function

  end #class

  class TestCase_PutObject_Huge < Test::Unit::TestCase
    def setup()
      Log.info("TestCase setup : #{self.name}", "blue_bg")
      assert_not_nil TestConfig.client
      @bucket_put_object_stream = "bucket-put-object-huge"
      assert(TestUtil.create_bucket_and_validate_ok(@bucket_put_object_stream))
      Log.info("OBJECT_MAX_FILESIZE=#{Object::MAX_OBJECT_FILE_SIZE} Bytes", "green")
      @max_object_size = Object::MAX_OBJECT_FILE_SIZE
    end

    def teardown()
      Log.info("TestCase teardown_suite : #{self.name}", "blue_bg")
      TestUtil.clean_one_bucket(@bucket_put_object_stream)
    end

    def test_put_object_stream_huge()
      bucket = TestConfig.client.get_bucket(@bucket_put_object_stream)
      o = [('0'..'9'),('a'..'z'),('A'..'Z')].map{|i| i.to_a}.flatten

      # Invalid Huge Stream
      Log.info("Test Invalid Huge Stream")
      object_name_invalid_huge_stream = "object-invalid-huge-stream"
      input_size = 0
      begin
        bucket.put_object(object_name_invalid_huge_stream) do |stream|
          while input_size<=@max_object_size
            diff = @max_object_size - input_size
            if diff>=1024
              s = (1..1024).map{ o[rand(o.length)] }.join
            else
              s = (0..diff).map{ o[rand(o.length)] }.join
            end
            stream << s
            input_size += s.size
          end
        end

        Log.info("object_size=#{input_size} Bytes", "blue")
        assert(false)
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        Log.info("object_size=#{input_size} Bytes", "yellow")
        assert(input_size>Object::MAX_OBJECT_FILE_SIZE)
        assert(true)
      end

      # Valid Huge Stream
      Log.info("Test Valid Huge Stream")
      object_name_huge_stream = "object-huge-stream-valid"
      input_size_target = @max_object_size
      input_size = 0
      begin
        bucket.put_object(object_name_huge_stream) do |stream|
          while input_size<input_size_target
            diff = input_size_target - input_size
            if diff>=1024
              s = (1..1024).map{ o[rand(o.length)] }.join
            else
              s = (1..diff).map{ o[rand(o.length)] }.join
            end

            stream << s
            input_size += s.size
          end
        end

        Log.info("object_size=#{input_size} Bytes", "blue")
        assert(input_size<=Object::MAX_OBJECT_FILE_SIZE)
        assert(true)
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        Log.info("object_size=#{input_size} Bytes", "yellow")
        assert(input_size<=Object::MAX_OBJECT_FILE_SIZE)
        assert(false)
      end

      # get object and validate size equal
      begin
        get_output = ""
        output_size = 0
        bucket.get_object(object_name_huge_stream) do |chunk|
          get_output = chunk
          output_size += chunk.size
        end
      rescue
        latest_error = $!.to_s
        Log.info(latest_error, "yellow")
        assert(false)
      end
      Log.info("put_object_size=#{input_size} Bytes, get_object_size=#{output_size} Bytes")
      assert_equal input_size, output_size
    end #function

  end #class

  class TestManagerObject
    def self.run()
      TestConfig.init()
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
      suite_list << test_suite_object

      suite_list.each do |suite|
        Test::Unit::UI::Console::TestRunner.run(suite)
        sleep(TestResource::TEST_INTERVAL)
      end
    rescue
      Log.last_error()
    end #function

  end #class

  TestManagerObject.run()

end #moduled