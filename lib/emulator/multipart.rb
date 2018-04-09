require 'builder'
require 'securerandom'
require "rexml/document"  
require 'emulator/config'
require 'emulator/util'
require 'emulator/request'
require 'emulator/response'
include REXML
include Comparable

module OssEmulator
  module Multipart
    
    # InitiateMultipartUpload
    def self.initiate_multipart_upload(bucket, object, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      dataset = {
        cmd: Request::POST_INIT_MULTIPART_UPLOAD, 
        bucket: bucket, 
        object: object, 
        upload_id: SecureRandom.hex
      }

      OssResponse.response_ok(response, dataset)
    end

    # UploadPart
    def self.upload_part(req, query, request, response) 
      part_number = query['partNumber'].first
      uploadId = query['uploadId'].first

      Object.put_object(req.bucket, req.object, request, response, part_number, uploadId)
    end

    # CompleteMultipartUpload
    def self.complete_multipart_upload(req, request, response)
      parts = []
      xml = Document.new(request.body)
      xml.elements.each("*/Part") do |e| 
        part = {}
        part[:number] = e.elements["PartNumber"].text
        etag = e.elements["ETag"].text
        part[:etag] = (etag.include?("&#34;")) ? etag[/&#34;(.+)&#34;/, 1] : etag
        parts << part
      end
      
      object_dir = File.join(Config.store, req.bucket, req.object)
      temp_subdir = req.query_parser['uploadId'].first
      temp_object_dir = File.join(object_dir, temp_subdir)
      base_obj_part_filename = File.join(temp_object_dir, Store::OBJECT_CONTENT_PREFIX)
      complete_file_size = 0
      parts.each do |part|
        part_filename = "#{base_obj_part_filename}#{part[:number]}"
        complete_file_size += File.size(part_filename)
      end

      options = { :temp_dir => temp_subdir, :size => complete_file_size, :part_size => File.size(File.join(temp_object_dir, Store::OBJECT_CONTENT)) }
      dataset = OssUtil.put_object_metadata(req.bucket, req.object, request, options)

      dataset[:cmd] = Request::POST_COMPLETE_MULTIPART_UPLOAD
      dataset[:object] = req.object
      OssResponse.response_ok(response, dataset)
    end #function

  end # class
end # module
