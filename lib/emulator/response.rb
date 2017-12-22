require 'yaml'
require 'find'
require 'time'
require 'fileutils'
require 'builder'
require "rexml/document"  
require 'emulator/config'
require 'emulator/chunkfile'
require 'emulator/util'
require 'emulator/request'
include REXML

module OssEmulator
  module OssResponse

    # Response ErrorCode to various failure
    def self.response_error(response, dataset=nil)
      if dataset.is_a?(Hash)
        error_code = dataset[:error_code]
      else
        Log.raise("response_error : Invalid parameter dataset : type is #{dataset.class}, value is #{dataset}")
      end

      request_id = OssUtil.generate_request_id()
      Log.fatal("response_error : error_code=#{error_code}, request_id=#{request_id}, dataset=#{dataset}\n", 'red')

      response.status = dataset[:status_code]
      response.header['Server'] = HttpMsg::ALIYUN_OSS_SERVER
      response.header['x-oss-request-id'] = request_id

      case error_code
      when ErrorCode::NO_SUCH_BUCKET[:error_code], ErrorCode::BUCKET_NOT_EMPTY[:error_code]
        response['Content-Type'] = 'application/xml'
        response.body = <<-eos.strip
          <?xml version="1.0" encoding="UTF-8"?>
          <Error>
            <Code>#{dataset[:error_code]}</Code>
            <Message>#{dataset[:message]}</Message>
            <RequestId>#{request_id}</RequestId>
            <HostId>#{Config.host}</HostId>
            <BucketName>#{dataset[:bucket_name]}</BucketName>
          </Error>
        eos
      else
        response['Content-Type'] = 'application/xml'
        response.body = <<-eos.strip
          <?xml version="1.0" encoding="UTF-8"?>
          <Error>
            <Code>#{dataset[:error_code]}</Code>
            <Message>#{dataset[:message]}</Message>
            <RequestId>#{request_id}</RequestId>
            <HostId>#{Config.host}</HostId>
          </Error>
        eos
      end #case
      
      Log.last_error()
    end #function

    # Response to get_object by chunk
    def self.response_get_object_by_chunk(response, dataset = {})
      response.status = 200
      request_id = OssUtil.generate_request_id()
      response['x-oss-request-id'] = request_id
      response['Server'] = HttpMsg::ALIYUN_OSS_SERVER

      Log.info("response_get_object_by_chunk : request_id=#{request_id}\n", "blue")
      Log.debug("response_get_object_by_chunk : request_id=#{request_id}, dataset=#{dataset}\n", 'blue')

      response['Content-Type'] = dataset[:content_type]
      if dataset[:content_encoding]
        response.header['X-Content-Encoding'] = dataset[:content_encoding]
        response.header['Content-Encoding'] = dataset[:content_encoding]
      end

      response['Content-Disposition'] = dataset[:content_disposition] if dataset[:content_disposition]
      response['Last-Modified'] = Time.parse(dataset[:modified_date]).strftime("%a, %d %b %Y %H:%M:%S GMT")
      response.header['ETag'] = "\"#{dataset[:md5]}\""
      response['Accept-Ranges'] = "bytes"
      response['Last-Ranges'] = "bytes"
      response['Access-Control-Allow-Origin'] = '*'
      dataset[:custom_metadata].each do |header, value|
        response.header['x-oss-meta-' + header] = value
      end

      object_dir = File.join(Config.store, dataset[:bucket], dataset[:object])
      object_content_filename = File.join(object_dir, Store::OBJECT_CONTENT)

      if !dataset[:multipart] # single part, whole 
        if dataset.include?('Content-Range') # single part with range
          Log.debug("response_get_object_by_chunk : request_id=#{request_id} : single part with Range", 'blue')
          response.status = 206
          response['Content-Range'] = dataset['Content-Range']

          options = { type: 'single_range', start_pos: dataset[:pos], read_length: dataset[:bytes_to_read] }
          response.body = ChunkFile.open(object_content_filename, options)
        else # single part without range
          Log.debug("response_get_object_by_chunk : request_id=#{request_id}: single part without Range", 'blue')

          options = { type: 'single_whole' }
          response.body = ChunkFile.open(object_content_filename, options)
        end
      else # multipart 
        if dataset.include?('Content-Range') # multipart with Range
          Log.debug("response_get_object_by_chunk : request_id=#{request_id}: multipart with Range", 'blue')
          response.status = 206
          response['Content-Range'] = dataset['Content-Range']

          options = { type: 'multipart_range', start_pos: dataset[:pos], read_length: dataset[:bytes_to_read], request_id: request_id,
                      base_part_filename: File.join(object_dir, "#{Store::OBJECT_CONTENT_PREFIX}")
                    }
          response.body = ChunkFile.open(object_content_filename, options)
          Log.debug("response_get_object_by_chunk : request_id=#{request_id}: multipart with Range end.", 'blue')
        else # multipart without Range
          Log.debug("response_get_object_by_chunk : request_id=#{request_id}: multipart without Range", 'blue')

          options = { type: 'multipart_whole', request_id: request_id, base_part_filename: File.join(object_dir, "#{Store::OBJECT_CONTENT_PREFIX}") }
          response.body = ChunkFile.open(object_content_filename, options)
        end
      end

    end # function response_get_object_by_chunk 

    # Response OK to various Request Method
    def self.response_ok(response, dataset={})
      response.status = 200
      request_id = OssUtil.generate_request_id()
      response['x-oss-request-id'] = request_id
      response['Server'] = HttpMsg::ALIYUN_OSS_SERVER

      cmd = ""
      if dataset.is_a?(String)
        cmd = dataset
      elsif dataset.is_a?(Hash) && dataset.include?(:cmd)
        cmd = dataset[:cmd]
      end
      Log.info("response_ok : request_id=#{request_id}\n", "green")
      Log.debug("response_ok : request_id=#{request_id}, dataset=#{dataset}\n", 'green')

      case cmd
      when Request::PUT_BUCKET
        response['Location'] = "oss-example"
        response['Access-Control-Allow-Origin'] = '*'
      when Request::GET_BUCKET_INFO
        response.header['x-oss-server-time'] = Time.now.strftime("%a, %d %b %Y %H:%M:%S GMT")
        response['Content-Type'] = 'application/xml'
        response.body = <<-eos.strip
          <?xml version="1.0" encoding="UTF-8"?>
          <BucketInfo>
            <Bucket>
              <CreationDate>#{dataset[:creation_date]}</CreationDate>
              <ExtranetEndpoint>oss-cn-hangzhou-zmf.aliyuncs.com</ExtranetEndpoint>
              <IntranetEndpoint>oss-cn-hangzhou-zmf-internal.aliyuncs.com</IntranetEndpoint>
              <Location>cn-hangzhou</Location>
              <Name>#{dataset[:bucket_name]}</Name>
              <Storage>Standard</Storage>
              <Owner>
                <DisplayName>1390402650033793</DisplayName>
                <ID>1390402650033793</ID>
              </Owner>
              <AccessControlList>
                <Grant>#{dataset[:acl]}</Grant>
              </AccessControlList>
            </Bucket>
          </BucketInfo>
        eos
      when Request::GET_BUCKET_ACL
        response['Content-Type'] = 'application/xml'
        response.body = <<-eos.strip
          <AccessControlPolicy>
            <Owner>
              <ID>#{HttpMsg::OWNER_ID}</ID>
              <DisplayName>#{HttpMsg::OWNER_DISPLAY_NAME}</DisplayName>
            </Owner>
            <AccessControlList>
              <Grant>private</Grant>
            </AccessControlList>
          </AccessControlPolicy>
        eos
      when Request::GET_BUCKET_LOCATION
        response['Content-Type'] = 'application/xml'
        response.body = <<-eos.strip
          <?xml version="1.0" encoding="UTF-8"?>
          <LocationConstraint>oss-cn-hangzhou</LocationConstraint>
        eos
      when Request::PUT_OBJECT
        response.header['ETag'] = "\"#{dataset[:md5]}\""
        response.header['x-oss-bucket-version'] = "1418321259"
      when Request::POST_APPEND_OBJECT
        response['x-oss-next-append-position'] = dataset['x-oss-next-append-position']
      when Request::DELETE_OBJECT
        response.status = 204
      when Request::DELETE_MULTIPLE_OBJECTS
        if dataset.include?(:object_list)
          xml_deleled_list = ""
          dataset[:object_list].each do |obj_name|
            xml_deleled_list += "<Deleted><Key>#{obj_name}</Key></Deleted>"
          end
          output = <<-eos.strip
            <?xml version="1.0" encoding="UTF-8"?><DeleteResult>#{xml_deleled_list}</DeleteResult>
          eos
          response.body = output
        end
      when Request::HEAD_OBJECT
        response['Accept-Ranges'] = 'bytes'
        response['ETag'] = dataset[:md5]
        response['Content-Length'] = dataset[:size]
        response['Last-Modified'] = Time.parse(dataset[:modified_date]).strftime("%a, %d %b %Y %H:%M:%S GMT")
        response['x-oss-object-type'] = 'Normal' 
        response['x-oss-storage-class'] = 'Standard'
        response['x-oss-server-time'] = Time.now.strftime("%a, %d %b %Y %H:%M:%S GMT")
      when Request::POST_INIT_MULTIPART_UPLOAD
        response['Content-Type'] = 'application/xml'
        response.body = <<-eos.strip
          <?xml version="1.0" encoding="UTF-8"?>
          <InitiateMultipartUploadResult>
            <Bucket>#{dataset[:bucket]}</Bucket>
            <Key>#{dataset[:object]}</Key>
            <UploadId>#{dataset[:upload_id]}</UploadId>
          </InitiateMultipartUploadResult>
        eos
      when Request::POST_COMPLETE_MULTIPART_UPLOAD
        response['Content-Type'] = 'application/xml'
        response.body = <<-eos.strip
          <?xml version="1.0" encoding="UTF-8"?>
          <CompleteMultipartUploadResult xmlns="http://doc.oss-cn-hangzhou.aliyuncs.com">
              <Location>Location</Location>
              <Bucket>oss-example</Bucket>
              <Key>#{dataset[:object]}</Key>
              <ETag>#{dataset[:md5]}</ETag>
          </CompleteMultipartUploadResult>
        eos
      when Request::GET_OBJECT
        self.response_get_object_by_chunk(response, dataset)
      when Request::PUT_COPY_OBJECT
        response['Content-Type'] = 'application/xml'
        output = ""
        xml = Builder::XmlMarkup.new(:target => output)
        xml.instruct! :xml, :version=>"1.0", :encoding=>"UTF-8"
        xml.CopyObjectResult(:xmlns => HttpMsg::XMLNS) { |result|
          result.LastModified(dataset[:modified_date])
          result.ETag("\"#{dataset[:md5]}\"")
        }
        response.body = output
      when Request::GET_OBJECT_META
        response['name'] = dataset[:name]
        response['md5'] = dataset[:md5]
        response['modified_date'] = dataset[:modified_date]
      when Request::GET_OBJECT_ACL
        response['Content-Type'] = 'application/xml'
        response['x-oss-server-time'] = Time.now.strftime("%a, %d %b %Y %H:%M:%S GMT")
        response.body = <<-eos.strip
          <?xml version="1.0" encoding="UTF-8"?>
          <AccessControlPolicy>
            <Owner>
              <ID>#{HttpMsg::OWNER_ID}</ID>
              <DisplayName>#{HttpMsg::OWNER_DISPLAY_NAME}</DisplayName>
            </Owner>
            <AccessControlList>
              <Grant>#{dataset[:acl]}</Grant>
            </AccessControlList>
          </AccessControlPolicy>
        eos
      when Request::POST_OBJECT
        response[:Etag] = dataset[:Etag]
        if dataset[:success_action_redirect]
          response.status      = 303
          response[:Location]  = dataset[:Location]
          response.body        = ""
        else
          response.status = dataset[:success_action_status] || 204
          if response.status == "201"
            response.body = <<-eos.strip
              <?xml version="1.0" encoding="UTF-8"?>
              <PostResponse>
                <Location>http://#{dataset[:bucket]}.localhost:80/#{dataset[:key]}</Location>
                <Bucket>#{dataset[:bucket]}</Bucket>
                <Key>#{dataset[:key]}</Key>
                <ETag>#{dataset[:Etag]}</ETag>
              </PostResponse>
            eos
          end
        end
      else
        if dataset.include?(:body)
          response.body = dataset[:body]
        end
      end #case
    end #function

    # Response while request method is OPTIONS 
    def self.response_options(response)
      response['Access-Control-Allow-Origin']   = '*'
      response['Access-Control-Allow-Methods']  = 'PUT, POST, HEAD, GET, OPTIONS'
      response['Access-Control-Allow-Headers']  = 'Accept, Content-Type, Authorization, Content-Length, ETag, X-CSRF-Token, Content-Disposition'
      response['Access-Control-Expose-Headers'] = 'ETag'
    end

    # NoSuchBucket
    def self.response_no_such_bucket(response, bucket)
      if !File.exist?(File.join(Config.store, bucket, Store::BUCKET_METADATA))
        dataset = { bucket: bucket }.merge(ErrorCode::NO_SUCH_BUCKET)
        OssResponse.response_error(response, dataset)
        return true
      end
      return false
    end

    # NoSuchBucket when DeleteBucket
    def self.response_no_such_bucket_when_delete_bucket(response, bucket)
      bucket_dir = File.join(Config.store, bucket)

      if !File.exist?(File.join(bucket_dir, Store::BUCKET_METADATA))
        if File.exist?(bucket_dir) && Dir[File.join(bucket_dir, '*')].length==0
          FileUtils.rm_rf(bucket_dir)
        end

        dataset = { bucket: bucket }.merge(ErrorCode::NO_SUCH_BUCKET)
        OssResponse.response_error(response, dataset)
        return true
      end

      return false
    end

    # BucketNotEmpty
    def self.response_bucket_not_empty(response, bucket)
      Find.find(File.join(Config.store, bucket)) do |filename|
        if filename.include?(Store::OBJECT_METADATA)
          dataset = { bucket: bucket }.merge(ErrorCode::BUCKET_NOT_EMPTY)
          OssResponse.response_error(response, dataset)
          return true
        end
      end

      return false
    end

    # InvalidBucketName
    def self.response_invalid_bucket_name(response, bucket)
      if !OssUtil.valid_bucket_name(bucket)
        dataset = { bucket: bucket }.merge(ErrorCode::INVALID_BUCKET_NAME)
        OssResponse.response_error(response, dataset)
        return true
      end
      return false
    end

    # TooManyBuckets
    def self.response_too_many_buckets(response)
      bucket_count = 0
      Dir[File.join(Config.store, "*")].each do |b|
        meta_file = File.join(b, Store::BUCKET_METADATA)
        if File.exist?(meta_file)
          bucket_count += 1
        end
      end

      if bucket_count>=Bucket::MAX_BUCKET_NUM
        OssResponse.response_error(response, ErrorCode::TOO_MANY_BUCKETS)
        return true
      end

      return false
    end
 
    # InvalidObjectName
    def self.response_invalid_object_name(response, object)
      object = object.force_encoding('UTF-8')
      if !OssUtil.valid_object_name(object)
        dataset = { object: object }.merge(ErrorCode::INVALID_OBJECT_NAME)
        OssResponse.response_error(response, dataset)
        return true 
      end
      return false
    end

    # NoSuchKey/NotFound
    def self.response_no_such_object(response, bucket, object, error_code = ErrorCode::NOT_FOUND[:error_code])
      if !File.exist?(File.join(Config.store, bucket, object, Store::OBJECT_METADATA))
        dataset = { bucket: bucket, object: object }.merge(ErrorCode::NOT_FOUND)
        OssResponse.response_error(response, dataset)
        return true
      end
      return false
    end

  end # module OssResponse
end # module OssEmulator