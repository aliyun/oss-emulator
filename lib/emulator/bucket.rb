require 'yaml'
require 'find'
require 'time'
require 'fileutils'
require 'builder'
require 'emulator/config'
require 'emulator/request'
require 'emulator/response'

module OssEmulator
  module Bucket

    # GetService=ListBuckets
    def self.get_service(response) 
      Bucket.list_buckets(response)
    end

    # ListBuckets=GetService
    def self.list_buckets(response) 
      body = ""
      xml = Builder::XmlMarkup.new(:target => body)
      xml.instruct! :xml, :version=>"1.0", :encoding=>"UTF-8"
      xml.ListAllMyBucketsResult(:xmlns => HttpMsg::XMLNS) { |lam|
        lam.Owner { |owner|
          owner.ID("00220120222")
          owner.DisplayName("1390402650033793")
        }
        lam.Buckets { |node|
          Dir[File.join(Config.store, "*")].each do |bucket|
            bucket_metadata_file = File.join(bucket, Store::BUCKET_METADATA)
            if File.exist?(bucket_metadata_file)
              node.Bucket do |sub_node|
                bucket_name = File.basename(bucket)
                sub_node.Name(bucket_name)
                tz = File.ctime(File.join(Config.store, bucket_name)).utc.iso8601(HttpMsg::SUBSECOND_PRECISION)
                sub_node.CreationDate(Time.parse(tz).strftime('%Y-%m-%dT%H:%M:%S.000Z'))
              end
            end
          end
        }
      }

      dataset = {
        cmd: Request::LIST_BUCKETS, 
        body: body
      }
      OssResponse.response_ok(response, dataset)
    end

    # PutBucket=CreateBucket
    def self.create_bucket(bucket, request, response)
      # InvalidBucketName
      return if OssResponse.response_invalid_bucket_name(response, bucket)

      # TooManyBuckets
      return if OssResponse.response_too_many_buckets(response)

      bucket_folder = File.join(Config.store, bucket)
      bucket_metadata_file = File.join(bucket_folder, Store::BUCKET_METADATA)
      if not ( File.exist?(bucket_folder) && File.exist?(bucket_metadata_file) )
        FileUtils.mkdir_p(bucket_folder)
        metadata = {}
        metadata[:bucket] = bucket
        metadata[:creation_date] = File.mtime(bucket_folder).utc.iso8601(HttpMsg::SUBSECOND_PRECISION)
        metadata[:acl] = request.header['x-oss-acl']
        File.open(bucket_metadata_file,'w') do |f|
          f << YAML::dump(metadata)
        end
      end
      
      OssResponse.response_ok(response, Request::PUT_BUCKET)
    end

    # PutBucketACL
    def self.put_bucket_acl(response) 
      OssResponse.response_ok(response)
    end

    # GetBucket=ListObjects
    def self.get_bucket(bucket, req, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      filter = {
        :marker => req.query["marker"] ? req.query["marker"].to_s : nil,
        :prefix => req.query["prefix"] ? req.query["prefix"].to_s : nil,
        :max_keys => req.query["max-keys"] ? req.query["max-keys"].to_i : nil,
        :delimiter => req.query["delimiter"] ? req.query["delimiter"].to_s : nil
      }

      body = ""
      xml = Builder::XmlMarkup.new(:target => body)
      xml.instruct! :xml, :version=>"1.0", :encoding=>"UTF-8"
      xml.ListBucketResult(:xmlns => HttpMsg::XMLNS) { |lbr|
        lbr.Name(bucket)
        lbr.Prefix(filter[:prefix])
        lbr.Marker(filter[:marker])
        lbr.MaxKeys("1000")
        lbr.Delimiter(filter[:delimiter])
        lbr.EncodingType("url")
        OssUtil.get_bucket_list_objects(lbr, req)
      }

      dataset = {
        :cmd => Request::GET_BUCKET, 
        :body => body
      }
      OssResponse.response_ok(response, dataset)
    end

    # GetBucketACL
    def self.get_bucetk_acl(bucket, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      OssResponse.response_ok(response, Request::GET_BUCKET_ACL)
    end

    # GetBucketLocation:  
    def self.get_bucket_location(response)
      OssResponse.response_ok(response, Request::GET_BUCKET_LOCATION)
    end

    # GetBucketInfo
    def self.get_bucetk_info(bucket, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      bucket_metadata_filename = File.join(Config.store, bucket, Store::BUCKET_METADATA)
      dataset = YAML.load(File.open(bucket_metadata_filename, 'rb').read)
      dataset[:bucket_name] = bucket
      dataset[:cmd] = Request::GET_BUCKET_INFO
      dataset[:acl] = "private"

      OssResponse.response_ok(response, dataset)
    end

    # DeleteBucket
    def self.delete_bucket(bucket, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket_when_delete_bucket(response, bucket)

      # BucketNotEmpty
      return if OssResponse.response_bucket_not_empty(response, bucket)

      # DeleteBucketFolder
      FileUtils.rm_rf(File.join(Config.store, bucket))
      OssResponse.response_ok(response)
    end

  end #class
end # module
