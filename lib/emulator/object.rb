require 'builder'
require "rexml/document"  
require 'emulator/config'
require 'emulator/util'
require 'emulator/response'
include REXML
include Comparable

module OssEmulator
  module Object

    # PutObject
    def self.put_object(bucket, object, request, response, part_number=nil, uploadId=nil)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      # InvalidObjectName
      return if OssResponse.response_invalid_object_name(response, object)

      check_chunked_filesize = false
      if request.header.include?('content-length')
        content_length = request.header['content-length'].first.to_i
        Log.debug("put_object : content_length=#{content_length}", 'blue')

        # InvalidArgument : Filesize <= 5G
        if content_length>Object::MAX_OBJECT_FILE_SIZE
          OssResponse.response_error(response, ErrorCode::INVALID_ARGUMENT)
          return  
        end
      else
        if request.header.include?('transfer-encoding')
          if request.header['transfer-encoding'].first!='chunked'
            # MissingContentLength
            OssResponse.response_error(response, ErrorCode::MISSING_CONTENT_LENGTH)
            return
          end
          check_chunked_filesize = true
          Log.debug("put_object : check_chunked_filesize=#{check_chunked_filesize}", 'blue')
        else
          # MissingContentLength
          OssResponse.response_error(response, ErrorCode::MISSING_CONTENT_LENGTH)
          return
        end
      end

      obj_dir = File.join(Config.store, bucket, object)
      temp_subdir = ""
      temp_obj_dir = ""
      Log.debug("request.header['authorization']=#{request.header['authorization']}")
      if part_number
        temp_subdir = uploadId
        Log.debug("temp_subdir=#{temp_subdir}", 'green')
        temp_obj_dir = File.join(obj_dir, temp_subdir)
        object_content_filename = File.join(temp_obj_dir, "#{Store::OBJECT_CONTENT_PREFIX}#{part_number}")
      else
        #OssUtil.delete_object_file_and_dir(bucket, object)
        temp_subdir = request.header['authorization'].first.split(':')[1].gsub(/[^a-zA-Z0-9]/, '')
        Log.debug("temp_subdir=#{temp_subdir}", 'green')
        temp_obj_dir = File.join(obj_dir, temp_subdir)
        object_content_filename = File.join(temp_obj_dir, Store::OBJECT_CONTENT)
      end
      FileUtils.mkdir_p(temp_obj_dir) unless File.exist?(temp_obj_dir)
      f_object_content = File.new(object_content_filename, 'a')  
      f_object_content.binmode

      content_type = request.content_type || ""
      match = content_type.match(/^multipart\/form-data; boundary=(.+)/)
      boundary = match[1] if match
      if boundary
        boundary = WEBrick::HTTPUtils::dequote(boundary)
        form_data = WEBrick::HTTPUtils::parse_form_data(request.body, boundary)

        if form_data['file'] == nil || form_data['file'] == ""
          OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
          return
        end

        f_object_content.syswrite(form_data['file'])
      else
        total_size = 0
        request.body do |chunk| 
          f_object_content.syswrite(chunk)
          total_size += chunk.bytesize
          if check_chunked_filesize && total_size>Object::MAX_OBJECT_FILE_SIZE
            OssUtil.delete_object_file_and_dir(bucket, object)
            OssResponse.response_error(response, ErrorCode::INVALID_ARGUMENT)
            return  
          end
        end
      end
      f_object_content.close()

      dataset = {}
      # put object metadata if not multipart upload
      option = { temp_dir: temp_subdir }
      dataset = OssUtil.put_object_metadata(bucket, object, request, option) unless part_number

      dataset[:cmd] = Request::PUT_OBJECT
      OssResponse.response_ok(response, dataset)
    end

    # CopyObject
    def self.copy_object(src_bucket, src_object, dst_bucket, dst_object, request, response)
      src_object_dir = File.join(Config.store, src_bucket, src_object)
      src_metadata_filename = File.join(src_object_dir, Store::OBJECT_METADATA)

      #temp_subdir = request.header['authorization'].first.split(':')[1].gsub(/[^a-zA-Z0-9]/, '')
      #dst_object_dir = File.join(Config.store, dst_bucket, dst_object, temp_subdir)
      dst_object_dir = File.join(Config.store, dst_bucket, dst_object)
      dst_metadata_filename = File.join(dst_object_dir, Store::OBJECT_METADATA)

      # NoSuchBucket : SrcBucket
      return if OssResponse.response_no_such_bucket(response, src_bucket)

      # NoSuchObject : SrcObject
      return if OssResponse.response_no_such_object(response, src_bucket, src_object)

      # Only update metadata if the src_object is the same as the dst_object
      if src_bucket==dst_bucket && src_object==dst_object
        metadata = OssUtil.put_object_metadata(dst_bucket, dst_object, request)
        metadata[:cmd] = Request::PUT_COPY_OBJECT
        OssResponse.response_ok(response, metadata)
        return
      end

      # Create New Bucket if the dst_bucket not exist
      dst_bucket_metadata_file = File.join(Config.store, dst_bucket, Store::BUCKET_METADATA)
      if !File.exist?(dst_bucket_metadata_file)
        dst_bucket_dir = File.join(Config.store, dst_bucket)
        FileUtils.mkdir_p(dst_bucket_dir)

        src_bucket_metadata_file = File.join(Config.store, src_bucket, Store::BUCKET_METADATA)
        metadata = File.open(src_bucket_metadata_file) { |file| YAML::load(file) }
        metadata[:bucket] = dst_bucket
        metadata[:creation_date] = Time.now.utc.iso8601(HttpMsg::SUBSECOND_PRECISION)
        File.open(dst_bucket_metadata_file,'w') do |f|
          f << YAML::dump(metadata)
        end
      end

      # Create dst_object folder if not exists.
      if !File.exist?(dst_object_dir)
        FileUtils.mkdir_p(dst_object_dir)
      end

      # Copy file content
      src_content_filename_base = File.join(src_object_dir, Store::OBJECT_CONTENT_PREFIX)
      dst_content_filename_base = File.join(dst_object_dir, Store::OBJECT_CONTENT_PREFIX)
      file_number = 1
      loop do
        current_src_filename = "#{src_content_filename_base}#{file_number}"
        break unless File.exist?(current_src_filename)
        current_dst_filename = "#{dst_content_filename_base}#{file_number}"
        File.open(current_dst_filename, 'wb') do |f|
          File.open(current_src_filename, 'rb') do |input|
            f << input.read
          end
        end
        file_number += 1
      end

      # Copy or Replace metadata
      metadata = {}
      metadata_directive = request.header["x-oss-metadata-directive"].first
      if metadata_directive == "REPLACE"
        metadata = OssUtil.put_object_metadata(dst_bucket, dst_object, request)
      else
        File.open(dst_metadata_filename, 'w') do |f|
          File.open(src_metadata_filename, 'r') do |input|
            f << input.read
          end
        end
        metadata = YAML.load(File.open(dst_metadata_filename, 'rb').read)
      end

      metadata[:cmd] = Request::PUT_COPY_OBJECT
      OssResponse.response_ok(response, metadata)
    end

    # GetObject
    def self.get_object(req, request, response)
      # NoSuchObject
      return if OssResponse.response_no_such_object(response, req.bucket, req.object)

      object_multipart_content_tag = File.join(Config.store, req.bucket, req.object, Store::OBJECT_CONTENT_TWO)
      object_metadata_filename = File.join(Config.store, req.bucket, req.object, Store::OBJECT_METADATA)
      metadata = File.open(object_metadata_filename) { |file| YAML::load(file) }
      dataset = {}
      dataset[:cmd] = Request::GET_OBJECT
      dataset[:bucket] = req.bucket
      dataset[:object] = req.object
      dataset[:md5] = metadata[:md5]
      dataset[:multipart] = File.exist?(object_multipart_content_tag) ? true : false 
      dataset[:content_type] = request.query['response-content-type'] || metadata.fetch(:content_type) { "application/octet-stream" }
      dataset[:content_disposition] = request.query['response-content-disposition'] || metadata[:content_disposition]
      dataset[:content_encoding] = metadata.fetch(:content_encoding)
      dataset[:size] = metadata.fetch(:size) { 0 }
      dataset[:part_size] = metadata.fetch(:part_size) { 0 }
      dataset[:creation_date] = metadata.fetch(:creation_date) { Time.now.utc.iso8601(HttpMsg::SUBSECOND_PRECISION) }
      dataset[:modified_date] = metadata.fetch(:modified_date) { Time.now.utc.iso8601(HttpMsg::SUBSECOND_PRECISION) }
      dataset[:custom_metadata] = metadata.fetch(:custom_metadata) { {} }

      # Range support
      range = request.header["range"].first
      if range
        Log.debug("get_object : request.header['range'].first=#{request.header['range'].first}", 'yellow')
        content_length = dataset[:size]
        if range =~ /bytes=(\d*)-(\d*)/
          start = $1.to_i
          finish = $2.to_i
          finish_str = ""
          if finish == 0
            finish = content_length - 1
            finish_str = "#{finish}"
          else
            finish_str = finish.to_s
          end

          dataset[:pos] = start
          dataset[:bytes_to_read] = finish - start + 1
          dataset['Content-Range'] = "bytes #{start}-#{finish_str}/#{content_length}"
        end 
      else
        dataset['Content-Length'] = dataset[:size]
      end #if range

      OssResponse.response_get_object_by_chunk(response, dataset)
    end #function

    # AppendObject
    def self.append_object(bucket, object, request, position, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      obj_dir = File.join(Config.store, bucket, object)
      metadata_filename = File.join(obj_dir, Store::OBJECT_METADATA)

      if File.exist?(metadata_filename)
        dataset = YAML.load(File.open(metadata_filename, 'rb').read)
        if !dataset.include?(:appendable) || (dataset.include?(:appendable) && dataset[:appendable]!=true)
          OssResponse.response_error(response, ErrorCode::OBJECT_NOT_APPENDABLE)
          return  
        end
      end

      FileUtils.mkdir_p(obj_dir)

      content_filename = File.join(obj_dir, Store::OBJECT_CONTENT)
      File.open(content_filename, 'a+')  do |f| 
        f.binmode
        f.pos = (position.to_i==-1) ? File.size(content_filename) : position.to_i
        f.syswrite(request.body)
      end

      options = { appendable: true }
      metadata = OssUtil.put_object_metadata(bucket, object, request, options)

      dataset = { cmd: Request::POST_APPEND_OBJECT }
      dataset['x-oss-next-append-position'] = (metadata[:size].to_i + 1).to_s
      OssResponse.response_ok(response, dataset)
    end

    # DeleteObject
    def self.delete_object(bucket, object, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      # delete object
      OssUtil.delete_object_file_and_dir(bucket, object)

      OssResponse.response_ok(response, Request::DELETE_OBJECT)
    end

    # DeleteMultipleObjects
    def self.delete_multiple_objects(bucket, request, response) 
      xml = Document.new(request.body)
      quiet = xml.root.elements["/Delete/Quiet"].text
      object_list = []
      xml.elements.each("*/Object/Key") do |e|
        object = e.text
        object_list << object
        Object.delete_object(bucket, object, response)
      end
      
      dataset = { cmd: Request::DELETE_MULTIPLE_OBJECTS }
      if quiet.downcase=="false"
        dataset[:object_list] = object_list      
      end

      OssResponse.response_ok(response, dataset)
    end

    # HeadObject
    def self.head_object(bucket, object, response) 
      # NoSuchObject
      return if OssResponse.response_no_such_object(response, bucket, object)

      object_metadata_filename = File.join(Config.store, bucket, object, Store::OBJECT_METADATA)
      dataset = File.open(object_metadata_filename) { |file| YAML::load(file) }
      dataset[:cmd] = Request::HEAD_OBJECT

      OssResponse.response_ok(response, dataset)
    end

    # GetObjectMeta
    def self.get_object_meta(bucket, object, request, response) 
      # NoSuchObject
      return if OssResponse.response_no_such_object(response, bucket, object)

      object_metadata_filename = File.join(Config.store, bucket, object, Store::OBJECT_METADATA)
      dataset = File.open(object_metadata_filename) { |file| YAML::load(file) }
      dataset[:cmd] = Request::GET_OBJECT_META

      OssResponse.response_ok(response, dataset)
    end

    # PutObjectACL
    def self.put_object_acl(bucket, object, request, response) 
      # NoSuchObject
      return if OssResponse.response_no_such_object(response, bucket, object)
      
      object_metadata_filename = File.join(Config.store, bucket, object, Store::OBJECT_METADATA)
      dataset = File.open(object_metadata_filename) { |file| YAML::load(file) }
      acl_old = dataset[:acl]
      dataset[:acl] = request.header["x-oss-object-acl"].first || acl_old
      File.open(object_metadata_filename,'w') do |f|
        f << YAML::dump(dataset)
      end
      dataset[:cmd] = Request::PUT_OBJECT_ACL

      OssResponse.response_ok(response, dataset)
    end

    # GetObjectACL
    def self.get_object_acl(bucket, object, response) 
      # NoSuchObject
      return if OssResponse.response_no_such_object(response, bucket, object)

      object_metadata_filename = File.join(Config.store, bucket, object, Store::OBJECT_METADATA)
      dataset = File.open(object_metadata_filename) { |file| YAML::load(file) }
      dataset[:cmd] = Request::GET_OBJECT_ACL
      dataset[:acl] = "default"

      OssResponse.response_ok(response, dataset)
    end

  end # class
end # module
